ActiveRecord::Base.class_eval do
  def self.bulk_insert_in_batches(attrs, options = {})
    batch_size = options.fetch(:batch_size, 1000)
    delay      = options.fetch(:delay, nil)
    max_packet = options.fetch(:max_packet, connection.execute("SHOW VARIABLES like 'max_allowed_packet';").to_h['max_allowed_packet'].to_i)

    attrs.each_slice(batch_size).map do |sliced_attrs|
      bulk_insert(sliced_attrs, options.merge(max_packet: max_packet))
      sleep(delay) if delay
    end.flatten.compact
  end

  def self.bulk_insert(attrs, options = {})
    return [] if attrs.empty?
    max_packet = options.fetch(:max_packet, connection.execute("SHOW VARIABLES like 'max_allowed_packet';").to_h['max_allowed_packet'].to_i)

    use_provided_primary_key = options.fetch(:use_provided_primary_key, false)
    attributes = _resolve_record(attrs.first, options).keys.join(", ")

    if options.fetch(:validate, false)
      attrs, invalid = attrs.partition { |record| _validate(record) }
    end

    values_sql = _generate_values_sql(attrs, options)

    insert_table = options[:insert_table] ? "`#{options[:insert_table]}`" : quoted_table_name

    sql = <<-SQL
      INSERT INTO #{insert_table}
        (#{attributes})
      VALUES
        values_sql
    SQL
    base_query_size = sql.bytesize
    if base_query_size + values_sql.bytesize > max_packet
      available = max_packet - base_query_size
      parts = (values_sql.bytesize / max_packet) + 1
      delay = options.fetch(:delay, nil)
      attrs.each_slice((attrs.length.to_f / parts).ceil) do |batch|
        batch_sql = _generate_values_sql(batch, options)
        connection.execute(sql.gsub('values_sql', batch_sql)) unless batch.empty?
        sleep(delay) if delay
      end
    else
      connection.execute(sql.gsub('values_sql', values_sql)) unless attrs.empty?
    end
    invalid
  end
  def self._generate_values_sql(attrs, options)
    attrs.map do |record|
      "(#{_resolve_record(record, options).values.map { |r| sanitize(r) }.join(', ')})"
    end.join(",")
  end

  def self._resolve_record(record, options)
    time = ActiveRecord::Base.default_timezone == :utc ? Time.now.utc : Time.now
    _record = record.is_a?(ActiveRecord::Base) ? record.attributes : record
    _record.merge!("created_at" => time, "updated_at" => time) unless options.fetch(:disable_timestamps, false)
    _record = _record.except(primary_key).except(primary_key.to_sym) unless options.fetch(:use_provided_primary_key, false)
    _record
  end

  def self._validate(record)
    if record.is_a?(Hash)
      new(record).valid?
    elsif record.is_a?(ActiveRecord::Base)
      record.valid?
    else
      false
    end
  end
end
