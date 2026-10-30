# frozen_string_literal: true

require "test_helper"
require "rails/generators/test_case"
require "generators/solid_queue/coalesce/install/install_generator"

class InstallGeneratorTest < Rails::Generators::TestCase
  tests SolidQueue::Coalesce::InstallGenerator
  destination Rails.root.join("tmp/install_generator_test")
  setup :prepare_destination

  test "copies the migration to the queue database migrations path" do
    run_generator

    assert_migration "db/queue_migrate/create_solid_queue_coalesce_tables.rb" do |migration|
      assert_match(/class CreateSolidQueueCoalesceTables/, migration)
      assert_match(/create_table :solid_queue_coalesce_keys/, migration)
      assert_match(/create_table :solid_queue_coalesced_arguments/, migration)
    end
  end

  test "copies the migration to another database's migrations path" do
    run_generator %w[ --database primary ]

    assert_migration "db/migrate/create_solid_queue_coalesce_tables.rb"
  end

  test "skips a migration that has already been copied" do
    run_generator
    run_generator

    assert_equal 1, Dir.glob(File.join(destination_root, "db/queue_migrate/*_create_solid_queue_coalesce_tables.rb")).count
  end
end
