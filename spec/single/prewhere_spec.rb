# frozen_string_literal: true

RSpec.describe 'PREWHERE', :migrations do
  class PrewhereJoin < ActiveRecord::Base
    self.table_name = 'joins'
  end

  class PrewhereModel < ActiveRecord::Base
    self.table_name = 'sample'
    has_many :joins, class_name: 'PrewhereJoin', foreign_key: 'event_name', primary_key: 'event_name'

    scope :enabled_only, -> { prewhere(enabled: true) }
  end

  before do
    migrations_dir = File.join(FIXTURES_PATH, 'migrations', 'add_sample_data')
    quietly { ActiveRecord::MigrationContext.new(migrations_dir).up }
    PrewhereModel.reset_column_information
  end

  def captured_sql
    statements = []
    callback = ->(*, payload) { statements << payload[:sql] }
    ActiveSupport::Notifications.subscribed(callback, 'sql.active_record') { yield }
    statements
  end

  describe 'SQL generation' do
    it 'renders a hash condition as PREWHERE' do
      sql = PrewhereModel.prewhere(event_name: 'a').to_sql
      expect(sql).to eq("SELECT sample.* FROM sample PREWHERE sample.event_name = 'a'")
    end

    it 'puts PREWHERE before WHERE regardless of call order' do
      expected = "SELECT sample.* FROM sample PREWHERE sample.enabled = TRUE WHERE sample.event_name = 'a'"
      expect(PrewhereModel.prewhere(enabled: true).where(event_name: 'a').to_sql).to eq(expected)
      expect(PrewhereModel.where(event_name: 'a').prewhere(enabled: true).to_sql).to eq(expected)
    end

    it 'joins multiple calls with AND' do
      sql = PrewhereModel.prewhere(enabled: true).prewhere('event_value > ?', 5).to_sql
      expect(sql).to eq('SELECT sample.* FROM sample PREWHERE sample.enabled = TRUE AND (event_value > 5)')
    end

    it 'accepts the same argument forms as #where' do
      table = PrewhereModel.arel_table
      expect(PrewhereModel.prewhere('event_value > :min', min: 3).to_sql)
        .to eq('SELECT sample.* FROM sample PREWHERE (event_value > 3)')
      expect(PrewhereModel.prewhere(['event_name = ?', 'x']).to_sql)
        .to eq("SELECT sample.* FROM sample PREWHERE (event_name = 'x')")
      expect(PrewhereModel.prewhere(table[:event_value].gt(1)).to_sql)
        .to eq('SELECT sample.* FROM sample PREWHERE sample.event_value > 1')
      expect(PrewhereModel.prewhere(event_name: %w[a b]).to_sql)
        .to eq("SELECT sample.* FROM sample PREWHERE sample.event_name IN ('a', 'b')")
      expect(PrewhereModel.prewhere(event_value: 1..3).to_sql)
        .to eq('SELECT sample.* FROM sample PREWHERE sample.event_value BETWEEN 1 AND 3')
    end

    it 'supports prewhere.not' do
      sql = PrewhereModel.prewhere.not(event_name: 'a').to_sql
      expect(sql).to eq("SELECT sample.* FROM sample PREWHERE sample.event_name != 'a'")
    end

    it 'ignores blank conditions like #where' do
      expect(PrewhereModel.prewhere(nil).to_sql).to eq('SELECT sample.* FROM sample')
      expect(PrewhereModel.prewhere({}).to_sql).to eq('SELECT sample.* FROM sample')
    end

    it 'does not modify the receiver' do
      base = PrewhereModel.where(event_name: 'a')
      base.prewhere(enabled: true)
      expect(base.to_sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name = 'a'")
    end

    it 'comes after FINAL' do
      sql = PrewhereModel.final.prewhere(enabled: true).to_sql
      expect(sql).to eq('SELECT sample.* FROM sample FINAL PREWHERE sample.enabled = TRUE')
    end

    it 'comes after JOINs' do
      sql = PrewhereModel.joins(:joins).prewhere(enabled: true).where(joins: { event_value: 1 }).to_sql
      expect(sql).to eq(
        'SELECT sample.* FROM sample INNER JOIN joins ON joins.event_name = sample.event_name ' \
        'PREWHERE sample.enabled = TRUE WHERE joins.event_value = 1'
      )
    end

    it 'composes with GROUP BY, ORDER BY, LIMIT and SETTINGS' do
      sql = PrewhereModel.prewhere(enabled: true).group(:event_name).order(:event_name).limit(5)
                         .settings(max_threads: 2).select('event_name, count() AS c').to_sql
      expect(sql).to eq(
        'SELECT event_name, count() AS c FROM sample PREWHERE sample.enabled = TRUE ' \
        'GROUP BY sample.event_name ORDER BY sample.event_name ASC LIMIT 5 SETTINGS max_threads = 2'
      )
    end

    it 'renders inside subqueries' do
      inner = PrewhereModel.prewhere(enabled: true).select(:event_name)
      sql = PrewhereJoin.where(event_name: inner).to_sql
      expect(sql).to eq(
        'SELECT joins.* FROM joins WHERE joins.event_name IN ' \
        '(SELECT sample.event_name FROM sample PREWHERE sample.enabled = TRUE)'
      )
    end

    it 'can be removed with unscope(:prewhere)' do
      sql = PrewhereModel.prewhere(enabled: true).where(event_name: 'a').unscope(:prewhere).to_sql
      expect(sql).to eq("SELECT sample.* FROM sample WHERE sample.event_name = 'a'")
    end

    it 'is carried through #merge and named scopes' do
      expect(PrewhereModel.where(event_name: 'a').merge(PrewhereModel.prewhere(enabled: true)).to_sql)
        .to eq("SELECT sample.* FROM sample PREWHERE sample.enabled = TRUE WHERE sample.event_name = 'a'")
      expect(PrewhereModel.where(event_name: 'a').enabled_only.to_sql)
        .to eq("SELECT sample.* FROM sample PREWHERE sample.enabled = TRUE WHERE sample.event_name = 'a'")
    end

    it 'lets a merged condition on the same column replace the original, like #where' do
      sql = PrewhereModel.prewhere(event_name: 'a').merge(PrewhereModel.prewhere(event_name: 'b')).to_sql
      expect(sql).to eq("SELECT sample.* FROM sample PREWHERE sample.event_name = 'b'")
    end
  end

  describe 'execution' do
    before do
      PrewhereModel.create!(event_name: 'a', event_value: 1, enabled: true, date: Date.current)
      PrewhereModel.create!(event_name: 'b', event_value: 2, enabled: false, date: Date.current)
      PrewhereModel.create!(event_name: 'c', event_value: 3, enabled: true, date: Date.current)
    end

    it 'filters rows' do
      expect(PrewhereModel.prewhere(enabled: true).order(:event_name).pluck(:event_name)).to eq(%w[a c])
      expect(PrewhereModel.prewhere(enabled: true).where('event_value > 1').pluck(:event_name)).to eq(%w[c])
      expect(PrewhereModel.prewhere.not(event_name: 'a').order(:event_name).pluck(:event_name)).to eq(%w[b c])
    end

    it 'works with count, exists? and FINAL' do
      expect(PrewhereModel.prewhere(enabled: true).count).to eq(2)
      expect(PrewhereModel.prewhere(event_name: 'zzz').exists?).to eq(false)
      expect(PrewhereModel.final.prewhere(enabled: true).count).to eq(2)
    end

    it 'moves PREWHERE into WHERE for delete_all instead of dropping it' do
      statements = captured_sql { PrewhereModel.prewhere(event_name: 'b').delete_all }
      expect(statements.last).to include("WHERE event_name = 'b'")
      expect(statements.last).not_to include('PREWHERE')
      expect(PrewhereModel.order(:event_name).pluck(:event_name)).to eq(%w[a c])
    end

    it 'moves PREWHERE into WHERE for update_all instead of dropping it' do
      statements = captured_sql { PrewhereModel.prewhere(event_name: 'b').where(enabled: false).update_all(event_value: 20) }
      expect(statements.last).to include("WHERE event_name = 'b' AND enabled = FALSE")
      expect(statements.last).not_to include('PREWHERE')
    end
  end
end
