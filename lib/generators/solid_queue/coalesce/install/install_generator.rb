# frozen_string_literal: true

require "rails/generators/active_record"

class SolidQueue::Coalesce::InstallGenerator < Rails::Generators::Base
  include ActiveRecord::Generators::Migration

  source_root File.expand_path("templates", __dir__)

  class_option :database, type: :string, aliases: %i[ --db ], default: "queue",
    desc: "The database that Solid Queue uses. Defaults to `queue`"

  def copy_migration
    migration_template "db/create_solid_queue_coalesce_tables.rb", File.join(db_migrate_path, "create_solid_queue_coalesce_tables.rb"), skip: true
  end
end
