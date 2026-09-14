# frozen_string_literal: true

class Widget < ActiveRecord::Base
  validates :name, presence: true, length: { maximum: 40 }
end
