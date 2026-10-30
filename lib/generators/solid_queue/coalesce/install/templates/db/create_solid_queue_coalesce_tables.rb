class CreateSolidQueueCoalesceTables < ActiveRecord::Migration[7.1]
  def change
    create_table :solid_queue_coalesce_keys do |t|
      # MySQL's default collations treat keys that differ by case or accent as the
      # same key, which would drop a call that should open its own job
      t.string :key, null: false, collation: ("utf8mb4_bin" if connection.adapter_name.match?(/mysql|trilogy/i))
      t.bigint :job_id
      t.datetime :created_at, null: false

      t.index :key, unique: true
      t.index :job_id, unique: true
      t.foreign_key :solid_queue_jobs, column: :job_id, on_delete: :cascade
    end

    create_table :solid_queue_coalesced_arguments do |t|
      t.bigint :coalesce_key_id
      t.bigint :job_id
      t.text :arguments, null: false
      t.datetime :scheduled_at
      t.datetime :created_at, null: false

      t.index :coalesce_key_id
      t.index :job_id
      t.foreign_key :solid_queue_coalesce_keys, column: :coalesce_key_id, on_delete: :cascade
      t.foreign_key :solid_queue_jobs, column: :job_id, on_delete: :cascade
    end
  end
end
