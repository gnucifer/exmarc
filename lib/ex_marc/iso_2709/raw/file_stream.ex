defmodule ExMarc.ISO2709.Raw.FileStream do
  # Other possible properties?
  @enforce_keys [:path]
  defstruct [:path, raw: true, read_ahead: true, compressed: false]

  @type t :: %__MODULE__{}

  @doc false
  def __build__(path, modes \\ []) do
    file_stream = [path: path]
    file_stream = case :lists.member(:read_ahead, modes) do
      true ->
        [{:read_ahead, true} | file_stream]
      false ->
        file_stream
    end
    file_stream = case :lists.member(:raw, modes) do
      true ->
        [{:raw, true} | file_stream]
      false ->
        file_stream
    end
    file_stream = case :lists.member(:compressed, modes) do
      true ->
        [{:compressed, true} | file_stream]
      false ->
        file_stream
    end
    #struct! ?
    struct(ExMarc.ISO2709.Raw.FileStream, file_stream)
    #%ExMarc.ISO2709.Raw.FileStream{path: path}
  end

  defimpl Enumerable do
    @read_ahead_size 64 * 1024

    # Implementation using FileStream.resource
    def reduce(%{path: path, raw: raw, read_ahead: read_ahead, compressed: compressed}, acc, fun) do
      start_fun = fn ->
        modes = [:read, :binary]
        modes = case raw do
          true ->
            [:raw | modes]
          false ->
            modes
        end
        modes = case read_ahead do
          true ->
            [{:read_ahead, @read_ahead_size} | modes]
          false ->
            modes
        end
        modes = case compressed do
          true ->
            [:compressed | modes]
            false
            -> modes
        end

        case :file.open(path, modes) do
          {:ok, device} ->
            device
          {:error, reason} ->
            raise File.Error, reason: reason, action: "stream", path: path #action: ???
        end
      end

      next_fun = fn device ->
        case ExMarc.ISO2709.Raw.read_record(device) do
          :eof ->
            {:halt, device} # Or done?
          {:error, reason} ->
            # TODO: Error module
            raise File.Error, reason: reason, action: "stream", path: path
          record -> {[record], device}
        end
      end

      Stream.resource(start_fun, next_fun, &:file.close/1).(acc, fun)
    end

    def count(_stream) do
      {:error, __MODULE__}
    end

    def member?(_stream, _term) do
      {:error, __MODULE__}
    end
  end
end
