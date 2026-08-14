defmodule SymphonyElixir.RuntimeIdentity do
  @moduledoc false

  @application :symphony_elixir

  @type evidence :: %{
          image_sha256: String.t(),
          execution_sha256: String.t()
        }

  @spec evidence(keyword()) :: {:ok, evidence()} | {:error, term()}
  def evidence(opts \\ []) do
    script_name_fn = Keyword.get(opts, :script_name_fn, &:escript.script_name/0)
    lstat_fn = Keyword.get(opts, :lstat_fn, &File.lstat/1)
    read_fn = Keyword.get(opts, :read_fn, &File.read/1)
    fingerprint_fn = Keyword.get(opts, :fingerprint_fn, &fingerprint/0)

    with {:ok, image_path} <- runtime_image_path(script_name_fn.()),
         :ok <- regular_runtime_image(lstat_fn.(image_path)),
         {:ok, image_bytes} <- read_runtime_image(read_fn.(image_path)),
         {:ok, execution_sha256} <- fingerprint_fn.(),
         :ok <- validate_execution_sha256(execution_sha256) do
      {:ok,
       %{
         image_sha256: sha256(image_bytes),
         execution_sha256: execution_sha256
       }}
    end
  end

  @spec write_manifest!(Path.t(), Path.t(), keyword()) :: :ok
  def write_manifest!(image_path, manifest_path, opts \\ []) do
    escript_path =
      Keyword.get_lazy(opts, :escript_path, fn ->
        System.find_executable("escript") || raise "escript executable is unavailable"
      end)

    clean_env_args = [
      "-i",
      "HOME=/var/empty",
      "LANG=C.UTF-8",
      "LC_ALL=C.UTF-8",
      "PATH=#{Path.dirname(escript_path)}:/usr/bin:/bin"
    ]

    {output, 0} =
      System.cmd(
        "/usr/bin/env",
        clean_env_args ++ [escript_path, Path.expand(image_path), "--runtime-identity"],
        stderr_to_stdout: true
      )

    ["image_sha256=" <> image_sha256, "execution_sha256=" <> execution_sha256] =
      String.split(output, "\n", trim: true)

    true = valid_sha256?(image_sha256)
    true = valid_sha256?(execution_sha256)
    {:ok, image_bytes} = File.read(image_path)
    ^image_sha256 = sha256(image_bytes)

    manifest =
      "image_sha256=#{image_sha256}\nexecution_sha256=#{execution_sha256}\n"

    temp_path = "#{manifest_path}.tmp.#{System.unique_integer([:positive])}"
    :ok = File.write(temp_path, manifest)
    :ok = File.chmod(temp_path, 0o600)
    :ok = File.rename(temp_path, manifest_path)
  end

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

    collect_application_modules([{@application, false}], [], [], load_fn, spec_fn)
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
    if application in seen do
      collect_application_modules(remaining, seen, modules, load_fn, spec_fn)
    else
      collect_unseen_application(
        application,
        optional?,
        remaining,
        seen,
        modules,
        load_fn,
        spec_fn
      )
    end
  end

  defp collect_unseen_application(
         application,
         optional?,
         remaining,
         seen,
         modules,
         load_fn,
         spec_fn
       ) do
    case normalize_application_load(load_fn.(application), application, optional?) do
      :skip ->
        collect_application_modules(
          remaining,
          [application | seen],
          modules,
          load_fn,
          spec_fn
        )

      :ok ->
        collect_loaded_application(application, remaining, seen, modules, load_fn, spec_fn)

      {:error, _reason} = error ->
        error
    end
  end

  defp collect_loaded_application(application, remaining, seen, modules, load_fn, spec_fn) do
    with {:ok, application_modules} <-
           application_spec_list(spec_fn, application, :modules, false),
         {:ok, dependencies} <- application_dependencies(spec_fn, application) do
      collect_application_modules(
        dependencies ++ remaining,
        [application | seen],
        application_modules ++ modules,
        load_fn,
        spec_fn
      )
    end
  end

  defp application_dependencies(spec_fn, application) do
    with {:ok, applications} <- application_spec_list(spec_fn, application, :applications, true),
         {:ok, included} <-
           application_spec_list(spec_fn, application, :included_applications, true),
         {:ok, optional} <-
           application_spec_list(spec_fn, application, :optional_applications, true) do
      {:ok, Enum.map(applications ++ included, &{&1, &1 in optional})}
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

  defp runtime_image_path(path) when is_binary(path) and byte_size(path) > 0, do: {:ok, path}
  defp runtime_image_path(path) when is_list(path) and path != [], do: {:ok, List.to_string(path)}
  defp runtime_image_path(_invalid), do: {:error, :runtime_identity_image_path_unavailable}

  defp regular_runtime_image({:ok, %File.Stat{type: :regular}}), do: :ok
  defp regular_runtime_image({:ok, _stat}), do: {:error, :runtime_identity_image_not_regular}

  defp regular_runtime_image({:error, reason}),
    do: {:error, {:runtime_identity_image_stat_failed, reason}}

  defp read_runtime_image({:ok, image_bytes}) when is_binary(image_bytes), do: {:ok, image_bytes}

  defp read_runtime_image({:error, reason}),
    do: {:error, {:runtime_identity_image_read_failed, reason}}

  defp validate_execution_sha256(value) do
    if valid_sha256?(value), do: :ok, else: {:error, :runtime_identity_execution_invalid}
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp valid_sha256?(value), do: is_binary(value) and value =~ ~r/\A[0-9a-fA-F]{64}\z/
end
