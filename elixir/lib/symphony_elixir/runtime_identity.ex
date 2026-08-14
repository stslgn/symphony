defmodule SymphonyElixir.RuntimeIdentity do
  @moduledoc false

  @application :symphony_elixir

  @spec fingerprint(keyword()) :: {:ok, String.t()} | {:error, term()}
  def fingerprint(opts \\ []) do
    with {:ok, modules} <- runtime_modules(opts),
         {:ok, identities} <- module_identities(modules, opts) do
      digest =
        identities
        |> Enum.sort_by(fn {module, _identity} -> Atom.to_string(module) end)
        |> Enum.map(fn {module, identity} ->
          name = Atom.to_string(module)
          [Integer.to_string(byte_size(name)), ":", name, identity]
        end)
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {:ok, digest}
    end
  end

  @spec verify(String.t() | nil, keyword()) :: {:ok, String.t()} | {:error, term()}
  def verify(expected, opts \\ []) do
    if valid_sha256?(expected) do
      fingerprint_fn = Keyword.get(opts, :fingerprint_fn, &fingerprint/0)

      with {:ok, actual} <- fingerprint_fn.() do
        compare(expected, actual)
      end
    else
      {:error, :missing_expected_runtime_identity}
    end
  end

  defp runtime_modules(opts) do
    case Keyword.fetch(opts, :modules) do
      {:ok, modules} when is_list(modules) -> {:ok, modules}
      {:ok, _invalid} -> {:error, :runtime_identity_modules_unavailable}
      :error -> application_modules(opts)
    end
  end

  defp application_modules(opts) do
    load_fn = Keyword.get(opts, :application_load_fn, &Application.load/1)
    spec_fn = Keyword.get(opts, :application_spec_fn, &Application.spec/2)

    collect_application_modules([{@application, false}], MapSet.new(), [], load_fn, spec_fn)
  end

  defp collect_application_modules([], _seen, modules, _load_fn, _spec_fn),
    do: {:ok, Enum.uniq(modules)}

  defp collect_application_modules(
         [{application, optional?} | remaining],
         seen,
         modules,
         load_fn,
         spec_fn
       ) do
    if MapSet.member?(seen, application) do
      collect_application_modules(remaining, seen, modules, load_fn, spec_fn)
    else
      case normalize_application_load(load_fn.(application), application, optional?) do
        :skip ->
          collect_application_modules(
            remaining,
            MapSet.put(seen, application),
            modules,
            load_fn,
            spec_fn
          )

        :ok ->
          with {:ok, application_modules} <-
                 application_spec_list(spec_fn, application, :modules, false),
               {:ok, dependencies} <- application_dependencies(spec_fn, application) do
            collect_application_modules(
              dependencies ++ remaining,
              MapSet.put(seen, application),
              application_modules ++ modules,
              load_fn,
              spec_fn
            )
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp application_dependencies(spec_fn, application) do
    with {:ok, applications} <- application_spec_list(spec_fn, application, :applications, true),
         {:ok, included} <-
           application_spec_list(spec_fn, application, :included_applications, true),
         {:ok, optional} <-
           application_spec_list(spec_fn, application, :optional_applications, true) do
      optional = MapSet.new(optional)
      {:ok, Enum.map(applications ++ included, &{&1, MapSet.member?(optional, &1)})}
    end
  end

  defp application_spec_list(spec_fn, application, key, nil_is_empty?) do
    case spec_fn.(application, key) do
      value when is_list(value) -> {:ok, value}
      nil when nil_is_empty? -> {:ok, []}
      _invalid -> {:error, {:runtime_identity_application_spec_invalid, application, key}}
    end
  end

  defp normalize_application_load(:ok, _application, _optional?), do: :ok

  defp normalize_application_load({:error, {:already_loaded, application}}, application, _optional?),
    do: :ok

  defp normalize_application_load(
         {:error, {~c"no such file or directory", _application_file}},
         _application,
         true
       ),
       do: :skip

  defp normalize_application_load({:error, reason}, application, _optional?),
    do: {:error, {:runtime_identity_application_load_failed, application, reason}}

  defp module_identities(modules, opts) do
    identity_fn = Keyword.get(opts, :module_identity_fn, &module_identity/1)

    Enum.reduce_while(modules, {:ok, []}, fn module, {:ok, identities} ->
      case identity_fn.(module) do
        {:ok, identity} when is_binary(identity) ->
          {:cont, {:ok, [{module, identity} | identities]}}

        {:ok, _invalid} ->
          {:halt, {:error, {:runtime_identity_module_invalid, module}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp module_identity(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} -> {:ok, module.module_info(:md5)}
      {:error, reason} -> {:error, {:runtime_identity_module_load_failed, module, reason}}
    end
  end

  defp compare(expected, actual) do
    normalized_expected = String.downcase(expected)

    if actual == normalized_expected do
      {:ok, actual}
    else
      {:error, {:runtime_identity_mismatch, normalized_expected, actual}}
    end
  end

  defp valid_sha256?(value), do: is_binary(value) and value =~ ~r/\A[0-9a-fA-F]{64}\z/
end
