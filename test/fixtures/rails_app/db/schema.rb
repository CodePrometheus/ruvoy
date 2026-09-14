# frozen_string_literal: true

ActiveRecord::Schema.define(version: 1) do
  create_table :widgets, if_not_exists: true do |t|
    t.string :name, null: false
    t.timestamps
  end
end
