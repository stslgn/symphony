defmodule SymphonyElixir.Linear.Comment do
  @moduledoc """
  Bounded Linear comment metadata used by the operator-command parser.

  Comment bodies are untrusted input. They are held only long enough to parse
  the leading command token and are never written to the run ledger.
  """

  defstruct [
    :id,
    :body,
    :created_at,
    author_is_me: false,
    external_thread_type: nil
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          body: String.t() | nil,
          created_at: DateTime.t(),
          author_is_me: boolean(),
          external_thread_type: String.t() | nil
        }
end
