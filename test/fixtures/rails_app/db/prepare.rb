# frozen_string_literal: true

# Creates the database on first use and puts the same rows back before every
# pass, so each server answers from identical data.
require_relative "../config/environment"

ActiveRecord::Migration.verbose = false
begin
  ActiveRecord::Base.with_connection(&:verify!)
rescue ActiveRecord::NoDatabaseError
  # Creating leaves the connection on the maintenance database.
  ActiveRecord::Tasks::DatabaseTasks.create(ActiveRecord::Base.connection_db_config)
  ActiveRecord::Base.establish_connection
end
load File.join(__dir__, "schema.rb")
ActiveRecord::Base.with_connection { it.execute("TRUNCATE widgets RESTART IDENTITY") }
%w[first second].each { Widget.create!(name: it) }
