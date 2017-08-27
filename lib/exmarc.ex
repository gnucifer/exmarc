defmodule ExMarc do

  @moduledoc """
  Documentation for ExMarc.
  """
  #@typedoc """
  #  TODO
  #"""

  defmodule ISO2708 do
    @leader_length 24
    @field_tag_length 3
    @directory_entry_length 12
    @record_terminator "\x1D"
    @field_terminator "\x1E" #Field terminator?
    @identifier_separator "\x1F" #identifier delimiter?

    # @type record :: list
    # @type records :: list(record)
    @type records :: list()

    @doc """
      Decode an iso2709 encoded binary into Elixir representation
    ## Examples
    """
    # @spec decode(IO.Stream.t) :: records
    # def file_stream_decode(filename) do
    #  filename
    #  |> File.stream!(read_ahead: 10_000_000, line_or_bytes: 2048) # TODO: Try varying this
    #  |> extract_records
    #end

    def file_decode(path) do
      #{:ok, file} = File.open(path, [:read, :binary, {:read_ahead, 64_000_000}])
      {:ok, file} = File.open(path, [:read, :binary, {:read_ahead, 64_000_000}])
      {:ok, records} = extract_records(file)
      :ok = File.close(file)
      records
      :ok
    end

    def extract_records(device) do
      {:ok, _extract_records(device, [])}
    end

    defp _extract_records(device, records) do
      case IO.binread(device, 24) do
        :eof ->
          records
        <<leader :: binary-size(@leader_length)>> -> # Rename to header?
          # {:ok, leader} = parse_header(data)

          # Guards for validation, is numeric?
          # TODO: length of implementation defined!
          <<
            record_length :: binary-size(5),
            _ :: binary-size(5),
            indicator_count :: binary-size(1),
            identifier_length :: binary-size(1),
            base_address_of_data :: binary-size(5),
            _ :: binary-size(3),
            length_of_field_length :: binary-size(1),
            length_of_starting_char_pos :: binary-size(1),
            #length_of_impl_defined :: binary-size(1),
            _ :: binary-size(2)
          >> = leader
          record_length = String.to_integer(record_length)
          base_address_of_data = String.to_integer(base_address_of_data)
          length_of_field_length = String.to_integer(length_of_field_length)
          length_of_starting_char_pos = String.to_integer(length_of_starting_char_pos)
          indicator_count = String.to_integer(indicator_count)
          identifier_length = String.to_integer(identifier_length)

          record_body = IO.binread(device, record_length - 24)
          directory_data_length = base_address_of_data - 25
          #fields_data_length = record_length - base_address_of_data - 1
          # - 1 because record terminator is included in record_length,
          # but we dont whant to include the terminator in the field data
          fields_data_length = record_length - base_address_of_data - 1
          #<<
          #  directory :: binary-size(directory_data_length),
          #  @field_terminator,
          #  fields_data :: binary-size(fields_data_length),
          #  wut :: binary
          #>> = record_body
          <<
            directory :: binary-size(directory_data_length),
            @field_terminator, # Marks end of directory
            fields_data :: binary-size(fields_data_length),
            @record_terminator
            >> = record_body

          #<<directory :: binary-size(directory_data_length), @field_terminator, rest :: binary>> = record_body
          #<<fields :: binary-size(fields_data_length), @record_terminator>> = rest
          directory_entries = parse_directory(directory)
          fields = parse_fields(
            #directory,
            directory_entries,
            fields_data,
            length_of_field_length,
            length_of_starting_char_pos,
            indicator_count,
            identifier_length
          )
          # Excluding record separator (TODO: rename to terminator if at end of every record)
          _extract_records(device, [{leader, fields, nil} | records]) #TODO: optional imp defined part?
        _ ->
          IO.puts "WTF" #TODO: error?
      end
    end

    # Does the one extra function call matters performance-wise?
    defp parse_fields(
      directory_entries,
      fields_data,
      length_of_field_length,
      length_of_starting_char_pos,
      indicator_count,
      identifier_length
    ) do
      me = self
      # We subract 1 from identifier length to get length excluding field terminator
      identifier_data_length = identifier_length - 1
      directory_entries
      #|> Enum.map(&Task.async(fn -> _parse_fields(&1, fields_data, length_of_field_length, length_of_starting_char_pos, indicator_count, identifier_data_length, []) end))
      #|> Enum.map(&Task.await(&1))
      |> Enum.map(fn (entry) ->
        spawn_link fn -> (send me, {self, _parse_fields(entry, fields_data, length_of_field_length, length_of_starting_char_pos, indicator_count, identifier_data_length, [])}) end
      end)
      |> Enum.map(fn (pid) ->
        receive do { ^pid, field } -> field end
      end)
      #{:ok, _parse_fields(
          #    directory_entries,
          #fields_data,
          #length_of_field_length,
          #length_of_starting_char_pos,
          #indicator_count,
          #identifier_length - 1, # We subtact 1 from identifier length to get length excluding field terminator
          #[]
          #)
          #}
    end

    # <<entry :: binary-size(@directory_entry_length), remaining_entries :: binary>>,
    # [entry | entries],
    # Rename to _parse_field
    defp _parse_fields(
      entry,
      fields_data,
      length_of_field_length,
      length_of_starting_char_pos,
      indicator_count,
      identifier_data_length,
      fields
    ) do
      #TODO: performance with one vs two pattern matches
      <<
        field_tag :: binary-size(@field_tag_length),
        field_length :: binary-size(length_of_field_length),
        starting_char_pos :: binary-size(length_of_starting_char_pos),
        _ :: binary
      >> = entry
      field_length = String.to_integer(field_length)
      starting_char_pos = String.to_integer(starting_char_pos)
      # Try add and remove to se effect on performance
      field_tag = to_charlist(field_tag)
      field_data_length = field_length - 1
      <<
        _ :: binary-size(starting_char_pos),
        field_data :: binary-size(field_data_length),
        @field_terminator,
        _ :: binary
      >> = fields_data
      # field_data = binary_part(fields_data, starting_char_pos, field_data_length)
      #field = parse_field(field_tag, field_data, indicator_count, identifier_length)
      field = if field_tag > '00Z' do
        {:ok, indicators_data, identifiers} = parse_bibliographic_field_data(
          field_data,
          indicator_count,
          identifier_data_length
        )
        {field_tag, indicators_data, identifiers}
      else
        {field_tag, field_data}
      end
      # TODO: remove this, can return if expression
      field
      #_parse_fields(
      #  #remaining_entries,
      #  entries,
      #  fields_data,
      #  length_of_field_length,
      #  length_of_starting_char_pos,
      #  indicator_count,
      #  identifier_data_length,
      #  [field | fields]
      #)
    end
    #defp _parse_fields(<<>>, _, _, _, _, _, fields) do
    #defp _parse_fields([], _, _, _, _, _, fields) do
    #  {:ok, fields}
    #end

    # Naming fubar
    #defp parse_field(tag, data, _, _) when tag <= '00Z' do
    #  {tag, data}
    #end

    #defp parse_field(tag, data, indicator_count, identifier_length) do
    #  {tag, _parse_bibliographic_field_data(tag, data, indicator_count, identifier_length, [])}
    #end

    def parse_bibliographic_field_data(data, indicator_count, identifier_data_length) do
      <<indicators_data :: binary-size(indicator_count), @identifier_separator, splittable_identifiers_data :: binary>> = data
      #TODO: convert identifier to atom
      #TODO: skip first separator??????
      #TODO: rename, is not separator but something else
      identifiers = for <<identifier :: binary-size(identifier_data_length), value :: binary>> <-
        splittable_identifiers_data |> String.splitter(@identifier_separator),
        #splittable_identifiers_data |> String.split(@identifier_separator),
        do: {String.to_atom(identifier), value}
      {:ok, indicators_data, identifiers}
    end

    defp parse_directory(data) do
      _parse_directory(data, [])
    end
    defp _parse_directory(<<entry :: binary-size(@directory_entry_length), remaining_entries :: binary>>, entries) do
      _parse_directory(remaining_entries, [entry | entries])
    end
    defp _parse_directory(<<>>, entries) do
      entries
    end

    defp parse_header(<<
        length :: binary-size(5),
        status :: binary-size(1),
        type_of_record :: binary-size(1),
        impl_defined_1 :: binary-size(2),
        character_coding_scheme :: binary-size(1),
        indicator_count :: binary-size(1),
        identifier_length :: binary-size(1),
        base_address_of_data :: binary-size(5),
        impl_defined_2 :: binary-size(3),
        length_of_length_of_field :: binary-size(1),
        length_of_starting_character_position :: binary-size(1),
        length_of_implementation_defined :: binary-size(1),
        _ :: binary-size(1) # Undefined
      >>) do
        {:ok, %{
            length: String.to_integer(length),
            status: status,
            type_of_record: type_of_record,
            impl_defined_1: impl_defined_1,
            character_coding_scheme: character_coding_scheme,
            indicator_count: String.to_integer(indicator_count),
            identifier_length: String.to_integer(identifier_length),
            base_address_of_data: String.to_integer(base_address_of_data),
            impl_defined_2: impl_defined_2,
            length_of_length_of_field: length_of_length_of_field,
            length_of_starting_character_position: length_of_starting_character_position,
            length_of_implementation_defined: length_of_implementation_defined
          }
        }
    end
    defp parse_header(<<data :: binary>>) do
      {:error, :invalid_leader, data} # I really have no idea!!
    end
    defp parse_header(<<>>) do
      {:error, :missing_leader}
    end

    # TODO: record, correct terminalogy?
    def decode_record(record) do
      [record]
    end
  end
end
