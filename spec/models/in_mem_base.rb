# frozen_string_literal: true

class InMemBase < ActiveRecord::Base
  self.abstract_class = true

  establish_connection(:in_mem)
  connects_to database: { writing: :in_mem }
end
