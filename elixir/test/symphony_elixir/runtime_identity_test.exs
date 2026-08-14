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
      application_load_fn: fn _application -> :ok end,
      application_spec_fn: fn
        :symphony_elixir, :modules -> [__MODULE__]
        :symphony_elixir, :applications -> []
        :symphony_elixir, :included_applications -> []
        :symphony_elixir, :optional_applications -> []
      end
    ]

    assert {:ok, digest} = RuntimeIdentity.fingerprint(loaded_opts)
    assert digest =~ ~r/\A[0-9a-f]{64}\z/

    already_loaded_opts =
      Keyword.put(loaded_opts, :application_load_fn, fn application ->
        {:error, {:already_loaded, application}}
      end)

    assert {:ok, ^digest} = RuntimeIdentity.fingerprint(already_loaded_opts)

    assert {:error, {:runtime_identity_application_load_failed, :symphony_elixir, :boom}} =
             RuntimeIdentity.fingerprint(application_load_fn: fn _application -> {:error, :boom} end)

    missing_optional_opts = [
      application_load_fn: fn
        :symphony_elixir -> :ok
        :missing_optional -> {:error, {~c"no such file or directory", ~c"missing_optional.app"}}
      end,
      application_spec_fn: fn
        :symphony_elixir, :modules -> [__MODULE__]
        :symphony_elixir, :applications -> [:missing_optional]
        :symphony_elixir, :included_applications -> []
        :symphony_elixir, :optional_applications -> [:missing_optional]
        :missing_optional, :modules -> []
        :missing_optional, :applications -> []
        :missing_optional, :included_applications -> []
        :missing_optional, :optional_applications -> []
      end
    ]

    assert {:ok, ^digest} = RuntimeIdentity.fingerprint(missing_optional_opts)

    assert {:error, {:runtime_identity_application_spec_invalid, :symphony_elixir, :modules}} =
             RuntimeIdentity.fingerprint(
               application_load_fn: fn _application -> :ok end,
               application_spec_fn: fn _application, _key -> nil end
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

  test "fingerprints bundled dependency modules as part of the execution identity" do
    root_identity = <<1::128>>
    dependency_a = <<2::128>>
    dependency_b = <<3::128>>

    graph_opts = [
      application_load_fn: fn _application -> :ok end,
      application_spec_fn: fn
        :symphony_elixir, :modules -> [String]
        :symphony_elixir, :applications -> [:runtime_dependency]
        :symphony_elixir, :included_applications -> []
        :symphony_elixir, :optional_applications -> []
        :runtime_dependency, :modules -> [Enum]
        :runtime_dependency, :applications -> [:symphony_elixir]
        :runtime_dependency, :included_applications -> nil
        :runtime_dependency, :optional_applications -> nil
      end
    ]

    identity_fn = fn dependency_identity ->
      fn
        String -> {:ok, root_identity}
        Enum -> {:ok, dependency_identity}
      end
    end

    assert {:ok, fingerprint_a} =
             RuntimeIdentity.fingerprint(Keyword.put(graph_opts, :module_identity_fn, identity_fn.(dependency_a)))

    assert {:ok, fingerprint_b} =
             RuntimeIdentity.fingerprint(Keyword.put(graph_opts, :module_identity_fn, identity_fn.(dependency_b)))

    refute fingerprint_a == fingerprint_b

    assert {:error, {:runtime_identity_application_spec_invalid, :runtime_dependency, :applications}} =
             RuntimeIdentity.fingerprint(
               Keyword.put(graph_opts, :application_spec_fn, fn
                 :symphony_elixir, :modules -> [String]
                 :symphony_elixir, :applications -> [:runtime_dependency]
                 :symphony_elixir, :included_applications -> []
                 :symphony_elixir, :optional_applications -> []
                 :runtime_dependency, :modules -> [Enum]
                 :runtime_dependency, :applications -> :invalid
                 :runtime_dependency, :included_applications -> []
                 :runtime_dependency, :optional_applications -> []
               end)
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
