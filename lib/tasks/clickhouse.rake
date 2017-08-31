namespace :clickhouse do

  namespace :schema do

    # todo not testing
    # desc 'Load database schema'
    # task load_schema: :environment do
    #   ActiveRecord::Base.establish_connection(:"#{Rails.env}_clickhouse")
    #   load("#{Rails.root}/db/clickhouse_schema.rb")
    # end

    desc 'Dump database schema'
    task dump: :environment do
      filename = "#{Rails.root}/db/clickhouse_schema.rb"
      File.open(filename, 'w:utf-8') do |file|
        ActiveRecord::Base.establish_connection(:"#{Rails.env}_clickhouse")
        ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection, file)
      end
    end

  end

end
