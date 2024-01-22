module CoreExtensions
  module ActiveRecord
    module SchemaMigration
      module ClassMethods

        def create_table
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

          return if table_exists?

          version_options = connection.internal_string_options_for_primary_key
          table_options = {
            id: false, options: 'ReplacingMergeTree(ver) ORDER BY (version)', if_not_exists: true
          }
          full_config = connection.instance_variable_get(:@full_config) || {}

          if full_config[:distributed_service_tables]
            table_options.merge!(with_distributed: table_name, sharding_key: 'cityHash64(version)')

            distributed_suffix = "_#{full_config[:distributed_service_tables_suffix] || 'distributed'}"
          else
            distributed_suffix = ''
          end

          connection.create_table(table_name + distributed_suffix.to_s, **table_options) do |t|
            t.string :version, **version_options
            t.column :active, 'Int8', null: false, default: '1'
            t.datetime :ver, null: false, default: -> { 'now()' }
          end
        end

        def delete_version(version)
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

          im = Arel::InsertManager.new(arel_table)
          im.insert(arel_table[primary_key] => version.to_s, arel_table['active'] => 0)
          connection.insert(im, "#{self.class} Create Rollback Version", primary_key, version)
        end

        def all_versions
          return super unless connection.is_a?(::ActiveRecord::ConnectionAdapters::ClickhouseAdapter)

          final.where(active: 1).order(:version).pluck(:version)
        end
      end
    end
  end
end
