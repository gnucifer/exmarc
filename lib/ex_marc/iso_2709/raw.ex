defmodule ExMarc.ISO2709.Raw do
  @leader_length 24
  @field_terminator "\x1E"
  @record_terminator "\x1D"

  def io_stream!(device) do
    ExMarc.ISO2709.Raw.IOStream.__build__(device)
  end

  def file_stream!(path, modes \\ []) do
    ExMarc.ISO2709.Raw.FileStream.__build__(path, modes)
  end

  def read_record(device) do
    case IO.binread(device, 24) do
      <<leader :: binary-size(@leader_length)>> -> # Rename to header?
        <<
          record_length :: binary-size(5), #attribute?
          _ :: binary-size(7),
          base_address_of_data :: binary-size(5),
          _ :: binary
        >> = leader
        #- 1 because record terminator is included in record_length,
        # but we don't want to include the terminator in the field data
        record_length = String.to_integer(record_length)
        base_address_of_data = String.to_integer(base_address_of_data)
        fields_data_length = record_length - base_address_of_data - 1
        directory_data_length = base_address_of_data - @leader_length - 1
        <<
          directory :: binary-size(directory_data_length),
          @field_terminator, # Marks end of directory
          fields_data :: binary-size(fields_data_length),
          @record_terminator
        >> = IO.binread(device, record_length - @leader_length)
        {leader, directory, fields_data}
      :eof -> :eof #Or nil? Let call handle, result -> result
      {:error, reason} = error -> error # Should probably throw error here instead, and less servere parsing errors should be returned
    end
  end

  def read_records(device, limit \\ -1) do
    _read_records(device, [], limit)
  end

  defp _read_records(_, records, 0) do
    records
  end

  defp _read_records(device, records, limit) do
    case read_record(device) do
      :eof ->
        records
      record -> # Rename to header?
        _read_records(
          device,
          [record | records],
          (if limit > 0, do: limit - 1, else: limit)
        )
    end
  end
end
