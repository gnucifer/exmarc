defmodule ExMarc.ISO2709.Stream do
  # Other possible properties?
  @enforce_keys :device
  defstruct device: nil

  @type t :: %__MODULE__{}

  @doc false
  def __build__(device) do
    %ExMarc.ISO2709.Stream{device: device}
  end

  defimpl Enumerable do
    # Implementation using Stream.resource
    def reduce(%{device: device}, acc, fun) do
      next_fun = fn device ->
        case ExMarc.ISO2709.read_raw_record(device) do
          :eof ->
            {:halt, device} # Or done?
          {:error, reason} ->
            # TODO: Error module
            #raise ExMarc.ISO2709.StreamError, reason: reason #?
            raise reason
          record -> {[record], device}
        end
      end
      Stream.resource(fn -> device end, next_fun, &(&1)).(acc, fun)
    end

    # Explicit implementation
    #def reduce(_, {:halt, acc}, _fun), do: {:halted, acc}
    #def reduce(stream, {:suspend, acc}, fun), do: {:suspended, acc, &reduce(stream, &1, fun)}
    #def reduce(%{device: device} = stream, {:cont, acc}, fun) do  #do: {:done, acc}
    #  case ExMarc.ISO2709.read_raw_record(device) do
    #    :eof ->
    #      {:done, acc}
    #    {:error, reason} ->
    #      raise reason
    #    record ->
    #      reduce(stream, fun.(record, acc), fun)
    #  end
    #end

    #def reduce([], {:cont, acc}, _fun), do: {:done, acc}
    #def reduce([h|t], {:cont, acc}, fun), do: reduce(t, fun.(h, acc), fun)

    def count(_stream) do
      {:error, __MODULE__}
    end
    def member?(_stream, _term) do
      {:error, __MODULE__}
    end
  end
end

