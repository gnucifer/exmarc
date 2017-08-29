defmodule ExMarc do

  @moduledoc """
  Documentation for ExMarc.
  """
  #@typedoc """
  #  TODO
  #"""

  defmodule ISO2709 do
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

    def file_decode(path) do
      {:ok, file} = File.open(path, [:read, :binary, {:read_ahead, 64_000_000}])
      #{:ok, file} = File.open(path, [:read, :binary, :raw])
      # TODO: extract -> decode pipeline
      records = file
                |> extract_records
                |> Enum.map(&ExMarc.ISO2709.decode_raw_record/1)
      :ok = File.close(file)
      records
    end

    def decode_raw_record({leader, directory_data, fields_data}) do
      <<
        _ :: binary-size(10), #attribute?
        indicator_count :: binary-size(1),
        identifier_length :: binary-size(1),
        _ :: binary-size(8),
        length_of_field_length :: binary-size(1),
        length_of_starting_char_pos :: binary-size(1),
        #length_of_impl_defined :: binary-size(1),
        _ :: binary-size(2)
      >> = leader
      length_of_field_length = String.to_integer(length_of_field_length)
      length_of_starting_char_pos = String.to_integer(length_of_starting_char_pos)
      indicator_count = String.to_integer(indicator_count)
      identifier_length = String.to_integer(identifier_length)

      #directory_entries = parse_directory(directory, length_of_field_length, length_of_starting_char_pos)
      directory_entries = for <<entry :: binary-size(@directory_entry_length) <- directory_data>> do
        <<
        field_tag :: binary-size(@field_tag_length),
          field_length :: binary-size(length_of_field_length),
          starting_char_pos :: binary-size(length_of_starting_char_pos),
          _ :: binary
        >> = entry
        {field_tag, String.to_integer(field_length), String.to_integer(starting_char_pos)}
      end
      fields_raw = for {field_tag, field_length, starting_char_pos} <- directory_entries do
        field_tag = to_charlist(:binary.copy(field_tag))
        #field_tag = to_charlist(field_tag)
        field_data_length = field_length - 1
        <<
          _ :: binary-size(starting_char_pos),
          field_data :: binary-size(field_data_length),
          @field_terminator,
          _ :: binary
        >> = fields_data
        {field_tag, :binary.copy(field_data)}
        #{field_tag, field_data}
      end

      # TODO: attribute for '00Z'?
      #{control_fields, bibliographic_fields_raw} = Enum.split_while(fields_raw, fn({field_tag, _}) -> field_tag <= '00Z' end)
      #{control_fields, bibliographic_fields_raw} = Enum.reduce(fields_raw, {[], []}, fn
      #  {field_tag, field_data} = field, {c_fields, bib_fields} when field_tag > '00Z' -> {c_fields, [field | bib_fields]}
      #  field, {c_fields, bib_fields} -> {[field | c_fields], bib_fields}
      #end)
      {bibliographic_fields_raw, control_fields} = Enum.split_with(fields_raw, fn({field_tag, _}) -> field_tag > '00Z' end)
      bibliographic_fields = decode_raw_bibliographic_fields(bibliographic_fields_raw, indicator_count, identifier_length)
      {leader, control_fields, bibliographic_fields, nil}
    end

    def extract_records(device) do
      _extract_records(device, [])
    end

    defp _extract_records(device, records) do
      case IO.binread(device, 24) do
        :eof ->
          records
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
          _extract_records(device, [{leader, directory, fields_data} | records])
        _ ->
          :error #TODO: What to do?
      end
    end

    defp decode_raw_bibliographic_fields(
      fields_raw,
      indicator_count,
      identifier_length
    ) do
      me = self()
      # We subtract 1 from identifier length to get length excluding field terminator
      identifier_data_length = identifier_length - 1
      fields_raw
      #|> Enum.map(&Task.async(fn -> _parse_field(&1, fields_data, indicator_count, identifier_data_length) end))
      #|> Enum.map(&Task.await(&1))
      # ---
      #|> Enum.map(fn ({field_tag, field_data}) ->
      #  spawn_link fn -> (send me, {self(), {field_tag, parse_bibliographic_field_data(field_data, indicator_count, identifier_data_length)}}) end
      #end)
      #|> Enum.map(fn (pid) ->
      #   receive do { ^pid, field } -> field end
      #end)
      # ---
      |> Enum.map(fn ({field_tag, field_data}) -> {field_tag, parse_bibliographic_field_data(field_data, indicator_count, identifier_data_length)} end)
    end

    # TODO: rename to decode
    defp parse_bibliographic_field_data(
      field_data,
      indicator_count,
      identifier_data_length
    ) do
      <<indicators_data :: binary-size(indicator_count), @identifier_separator, splittable_identifiers_data :: binary>> = field_data
      identifiers = for <<identifier :: binary-size(identifier_data_length), value :: binary>> <-
        splittable_identifiers_data |> String.splitter(@identifier_separator),
        #splittable_identifiers_data |> String.split(@identifier_separator),
        do: {identifier, value}
      {indicators_data, identifiers}
    end

    defp parse_directory(data, length_of_field_length, length_of_starting_char_pos) do
      _parse_directory(data, length_of_field_length, length_of_starting_char_pos, [])
    end

    # Replace with chunks and list comprehension and compare performance
    defp _parse_directory(
      <<entry :: binary-size(@directory_entry_length), remaining_entries :: binary>>,
      length_of_field_length,
      length_of_starting_char_pos,
      entries
    ) do
      <<
        field_tag :: binary-size(@field_tag_length),
        field_length :: binary-size(length_of_field_length),
        starting_char_pos :: binary-size(length_of_starting_char_pos),
        _ :: binary
      >> = entry
      _parse_directory(
        remaining_entries,
        length_of_field_length,
        length_of_starting_char_pos,
        [{field_tag, String.to_integer(field_length), String.to_integer(starting_char_pos)} | entries]
      )
    end
    defp _parse_directory(<<>>, _, _, entries) do
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
  end
end
