defmodule ExMarc.ISO2709 do
  @field_tag_length 3
  @directory_entry_length 12
  @field_terminator "\x1E" #Field terminator?
  @identifier_separator "\x1F" #identifier delimiter?
  @read_ahead_size 64 * 1024

  @doc """
    Decode an iso2709 encoded binary into Elixir representation
  ## Examples
  """

  defmodule Record do
    @enforce_keys [:leader, :contril_fields, :bibliographic_fields]
    defstruct [:leader, :control_fields, :bibliographic_fields, impl_part: nil]

    #defimpl Enumerable do
    #	def reduce(_,     {:halt, acc}, _fun),   do: {:halted, acc}
    #	def reduce(list,  {:suspend, acc}, fun), do: {:suspended, acc, &reduce(list, &1, fun)}
    #	def reduce([],    {:cont, acc}, _fun),   do: {:done, acc}
    #	def reduce([h|t], {:cont, acc}, fun),    do: reduce(t, fun.(h, acc), fun)

    #	def member?(_list, _value),
    #		do: {:error, __MODULE__}
    #	def count(_list),
    #   do: {:error, __MODULE__}
    #end
  end

  def io_stream!(device) do
    device
    |> ExMarc.ISO2709.Raw.io_stream!()
    |> Stream.map(&ExMarc.ISO2709.decode_raw_record/1)
  end

  def file_stream!(path, modes \\ []) do
    path
    |> ExMarc.ISO2709.Raw.file_stream!(modes)
    |> Stream.map(&ExMarc.ISO2709.decode_raw_record/1)
  end

  def file_decode!(path) do
    # :raw?
    file = File.open!(path, [:raw, :read, :binary, {:read_ahead, @read_ahead_size}])
    #{:ok, file} = File.open(path, [:read, :binary, :raw])
    # TODO: extract -> decode pipeline
    #me = self()
    #spawn_link fn -> (send me, read_raw_records(file)) end
    #records = receive do raw_records -> (raw_records |> Enum.map(&ExMarc.ISO2709.decode_raw_record/1)) end
    # raise File.Error, reason: reaosn, action; "stream", path: path?
    records = file
              |> ExMarc.ISO2709.Raw.read_records
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
      field_tag = to_charlist(field_tag)
      field_data_length = field_length - 1
      <<
        _ :: binary-size(starting_char_pos),
        field_data :: binary-size(field_data_length),
        @field_terminator,
        _ :: binary
      >> = fields_data
      {field_tag, field_data}
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
    |> Enum.map(fn ({field_tag, field_data}) ->
      spawn_link fn -> (send me, {self(), {field_tag, parse_bibliographic_field_data(field_data, indicator_count, identifier_data_length)}}) end
    end)
    |> Enum.map(fn (pid) ->
       receive do { ^pid, field } -> field end
    end)
    # ---
    # |> Enum.map(fn ({field_tag, field_data}) -> {field_tag, parse_bibliographic_field_data(field_data, indicator_count, identifier_data_length)} end)
  end

  # TODO: rename to decode
  defp parse_bibliographic_field_data(
    field_data,
    indicator_count,
    identifier_data_length
  ) do
    # TODO: Can this be done outside of function to avoid recompliation on each call? Investingate:
    # Compile pattern for performance
    identifier_separator_pattern = :binary.compile_pattern(@identifier_separator)
    <<indicators_data :: binary-size(indicator_count), @identifier_separator, splittable_identifiers_data :: binary>> = field_data
    identifiers = for <<identifier :: binary-size(identifier_data_length), value :: binary>> <-
      #splittable_identifiers_data |> String.splitter(@identifier_separator),
      splittable_identifiers_data |> String.split(identifier_separator_pattern),
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

  # Rename to GenStageReader? rename read_raw_records to read_raw_records?
  defmodule GenStageExtractor do
    use GenStage

    def start_link(device) do
      GenStage.start_link(__MODULE__, device, name: __MODULE__)
    end

    def init(device) do
      {:producer, device}
    end

    def handle_demand(demand, device) when demand > 0 do
      events = ExMarc.ISO2709.Raw.read_records(device, demand)
      # TODO: probably need halt condition here
      {:noreply, events, device}
    end
  end

  defmodule GenStageDecoder do
    use GenStage

    def start_link(concurrency_level) do
      GenStage.start_link(__MODULE__, concurrency_level, name: __MODULE__)
    end

    def init(concurrency_level) do
      #{:producer_consumer, concurrency_level}
      {:producer_consumer, concurrency_level, subscribe_to: [ExMarc.ISO2709.GenStageExtractor]}
    end
    # Naming events for now, but should probably change to raw_records
    def handle_events(events, _from, concurrency_level) do
      #TODO: what happened with concurrency_level :)
      # pararallel map etc, partition events by c-level and map flatten?
      events = events |> Enum.map(&ExMarc.ISO2709.decode_raw_record/1)
      {:noreply, events, concurrency_level} # WTF record
    end
  end

  # Just for testing
  defmodule GenStageConsumer do
    use GenStage

    def start_link(sleeping_time) do
      GenStage.start_link(__MODULE__, sleeping_time)
    end

    def init(sleeping_time) do
      # WHOLE MOULD ENAME?
      {:consumer, sleeping_time, subscribe_to: [ExMarc.ISO2709.GenStageDecoder]}
    end

    def handle_events(events, _from, sleeping_time) do
      IO.puts(length(events))
      #IO.inspect(Enum.take(events, 1) |> elem(0))
      Process.sleep(sleeping_time)
      # We are a consumer, so we never emit events
      {:noreply, [], sleeping_time}
    end
  end

  #def gen_stage_test_file(file) do
  #end

  def gen_stage_test(device, concurrency_level \\ 4) do
    {:ok, extractor} = GenStageExtractor.start_link(device)
    {:ok, decoder} = GenStageDecoder.start_link(concurrency_level)
    {:ok, consumer} = GenStageConsumer.start_link(100)

    #GenStage.sync_subscribe(consumer, to: decoder)
    #GenStage.sync_subscribe(decoder, to: extractor)
    #Process.sleep(:infinity) #?
  end
end
