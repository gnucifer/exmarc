defmodule ExMarc.CLI do
  def main([filename | _]) do
    ExMarc.ISO2709.file_decode(filename)
  end
end
