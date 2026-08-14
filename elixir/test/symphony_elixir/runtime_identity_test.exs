defmodule SymphonyElixir.RuntimeIdentityTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RuntimeIdentity

  test "fingerprints a stable sorted set of loaded module identities" do
    identities = %{
      String => <<1::128>>,
      Enum => <<2::128>>
    }

    opts = [
      modules: [String, Enum],
      module_identity_fn: fn module -> {:ok, Map.fetch!(identities, module)} end
    ]

    assert {:ok, first} = RuntimeIdentity.fingerprint(opts)
    assert {:ok, ^first} = RuntimeIdentity.fingerprint(Keyword.put(opts, :modules, [Enum, String]))
    assert first =~ ~r/\A[0-9a-f]{64}\z/
  end

  test "loads the application manifest and rejects incomplete runtime identity evidence" do
    loaded_opts = [
      application_load_fn: fn -> :ok end,
      application_modules_fn: fn -> [__MODULE__] end
    ]

    assert {:ok, digest} = RuntimeIdentity.fingerprint(loaded_opts)
    assert digest =~ ~r/\A[0-9a-f]{64}\z/

    already_loaded_opts =
      Keyword.put(loaded_opts, :application_load_fn, fn -> {:error, {:already_loaded, :symphony_elixir}} end)

    assert {:ok, ^digest} = RuntimeIdentity.fingerprint(already_loaded_opts)

    assert {:error, {:runtime_identity_application_load_failed, :boom}} =
             RuntimeIdentity.fingerprint(application_load_fn: fn -> {:error, :boom} end)

    assert {:error, :runtime_identity_modules_unavailable} =
             RuntimeIdentity.fingerprint(
               application_load_fn: fn -> :ok end,
               application_modules_fn: fn -> nil end
             )

    assert {:error, :runtime_identity_modules_unavailable} =
             RuntimeIdentity.fingerprint(modules: :invalid)

    assert {:error, {:runtime_identity_module_load_failed, MissingRuntimeIdentityModule, :nofile}} =
             RuntimeIdentity.fingerprint(modules: [MissingRuntimeIdentityModule])

    assert {:error, {:runtime_identity_module_invalid, String}} =
             RuntimeIdentity.fingerprint(
               modules: [String],
               module_identity_fn: fn _module -> {:ok, :invalid} end
             )
  end

  test "verifies the expected loaded-code identity rather than a pathname claim" do
    expected = String.duplicate("a", 64)

    assert {:ok, actual_runtime_identity} = RuntimeIdentity.fingerprint()
    assert {:ok, ^actual_runtime_identity} = RuntimeIdentity.verify(actual_runtime_identity)

    assert {:ok, ^expected} =
             RuntimeIdentity.verify(expected, fingerprint_fn: fn -> {:ok, expected} end)

    assert {:error, :missing_expected_runtime_identity} =
             RuntimeIdentity.verify(nil, fingerprint_fn: fn -> {:ok, expected} end)

    assert {:error, :missing_expected_runtime_identity} =
             RuntimeIdentity.verify("invalid", fingerprint_fn: fn -> {:ok, expected} end)

    assert {:error, {:runtime_identity_mismatch, ^expected, actual}} =
             RuntimeIdentity.verify(expected,
               fingerprint_fn: fn -> {:ok, String.duplicate("b", 64)} end
             )

    assert actual == String.duplicate("b", 64)

    assert {:error, :runtime_identity_probe_failed} =
             RuntimeIdentity.verify(expected,
               fingerprint_fn: fn -> {:error, :runtime_identity_probe_failed} end
             )
  end
end
