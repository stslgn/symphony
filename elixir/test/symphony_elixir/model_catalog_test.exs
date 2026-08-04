defmodule SymphonyElixir.ModelCatalogTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Codex.ModelCatalog

  test "discovers paginated visible models and preserves catalog order" do
    parent = self()

    request_fun = fn request_id, method, params ->
      send(parent, {:request, request_id, method, params})

      case request_id do
        10_000 ->
          {:ok,
           %{
             "data" => [
               %{
                 "model" => "gpt-default",
                 "isDefault" => true,
                 "defaultReasoningEffort" => "medium",
                 "supportedReasoningEfforts" => [
                   %{"reasoningEffort" => "low"},
                   %{"reasoningEffort" => "medium"},
                   %{"reasoningEffort" => "medium"},
                   %{"effort" => "high"}
                 ]
               },
               %{"model" => "gpt-default", "isDefault" => false},
               %{"model" => "bad\nmodel"},
               %{"id" => "missing-model"}
             ],
             "nextCursor" => "next-page"
           }}

        10_001 ->
          {:ok,
           %{
             "data" => [
               %{
                 "model" => "gpt-fast",
                 "isDefault" => false,
                 "defaultReasoningEffort" => "low",
                 "supportedReasoningEfforts" => [%{"reasoningEffort" => "low"}],
                 "upgrade" => "gpt-default"
               }
             ],
             "nextCursor" => nil
           }}
      end
    end

    assert {:ok, catalog} = ModelCatalog.discover(request_fun)
    assert catalog.source == :live
    assert catalog.default_model == "gpt-default"
    assert Enum.map(catalog.models, & &1.model) == ["gpt-default", "gpt-fast"]

    assert hd(catalog.models).supported_reasoning_efforts == ["low", "medium", "high"]
    assert List.last(catalog.models).upgrade == "gpt-default"

    assert_receive {:request, 10_000, "model/list", %{"cursor" => nil, "includeHidden" => false}}

    assert_receive {:request, 10_001, "model/list", %{"cursor" => "next-page", "includeHidden" => false}}

    payload = ModelCatalog.payload(catalog)
    assert payload.source == "live"
    assert payload.default_model == "gpt-default"
    assert is_binary(payload.fetched_at)
    assert payload.error == nil
    assert Enum.map(payload.models, & &1.model) == ["gpt-default", "gpt-fast"]
  end

  test "validates the actual thread model and reasoning effort against the live catalog" do
    catalog = %{
      source: :live,
      models: [
        %{
          model: "gpt-live",
          default?: true,
          default_reasoning_effort: "medium",
          supported_reasoning_efforts: ["low", "medium", "high"],
          upgrade: nil
        }
      ],
      default_model: "gpt-live",
      fetched_at: DateTime.utc_now(),
      error: nil
    }

    assert :ok = ModelCatalog.validate_resolution(catalog, "gpt-live", "high")
    assert :ok = ModelCatalog.validate_resolution(catalog, "gpt-live", nil)

    assert {:error, :resolved_model_missing} =
             ModelCatalog.validate_resolution(catalog, nil, "high")

    assert {:error, {:model_not_available, "gpt-missing"}} =
             ModelCatalog.validate_resolution(catalog, "gpt-missing", "high")

    assert {:error, {:reasoning_effort_not_supported, "gpt-live", "ultra", ["low", "medium", "high"]}} =
             ModelCatalog.validate_resolution(catalog, "gpt-live", "ultra")

    assert :ok =
             ModelCatalog.validate_resolution(ModelCatalog.unavailable(:response_timeout), "gpt-missing", "ultra")
  end

  test "rejects malformed or empty catalogs and bounds failure details" do
    assert {:error, :empty_model_catalog} =
             ModelCatalog.discover(fn _id, _method, _params ->
               {:ok, %{"data" => [], "nextCursor" => nil}}
             end)

    assert {:error, :invalid_model_catalog} =
             ModelCatalog.discover(fn _id, _method, _params -> {:ok, %{"data" => "bad"}} end)

    assert {:error, :boom} =
             ModelCatalog.discover(fn _id, _method, _params -> {:error, :boom} end)

    assert ModelCatalog.error_code(:response_timeout) == :response_timeout
    assert ModelCatalog.error_code({:port_exit, 1}) == :app_server_exit
    assert ModelCatalog.error_code({:response_error, %{"message" => "secret-like detail"}}) == :request_failed
    assert ModelCatalog.error_code(:empty_model_catalog) == :empty_model_catalog
    assert ModelCatalog.error_code(:catalog_page_limit) == :catalog_page_limit
    assert ModelCatalog.error_code(:catalog_model_limit) == :catalog_model_limit
    assert ModelCatalog.error_code(:anything_else) == :discovery_failed

    assert ModelCatalog.payload(ModelCatalog.unavailable({:response_error, %{sensitive: true}})) == %{
             source: "unavailable",
             default_model: nil,
             fetched_at: nil,
             error: "request_failed",
             models: []
           }
  end

  test "rejects catalogs that exceed the bounded public model surface" do
    models =
      Enum.map(1..129, fn index ->
        %{"model" => "gpt-bounded-#{index}", "supportedReasoningEfforts" => []}
      end)

    assert {:error, :catalog_model_limit} =
             ModelCatalog.discover(fn _id, _method, _params ->
               {:ok, %{"data" => models, "nextCursor" => nil}}
             end)
  end

  test "bounds pagination and sanitizes malformed optional catalog fields" do
    assert {:error, :catalog_page_limit} =
             ModelCatalog.discover(fn request_id, _method, _params ->
               {:ok,
                %{
                  "data" => [%{"model" => "gpt-page-#{request_id}"}],
                  "nextCursor" => "more"
                }}
             end)

    assert {:error, :invalid_model_catalog} =
             ModelCatalog.discover(fn _id, _method, _params -> {:ok, %{"models" => []}} end)

    assert {:ok, catalog} =
             ModelCatalog.discover(fn _id, _method, _params ->
               {:ok,
                %{
                  "data" => [
                    %{"model" => 42},
                    %{
                      "model" => "gpt-sanitized",
                      "defaultReasoningEffort" => 42,
                      "supportedReasoningEfforts" => [
                        %{"reasoningEffort" => 42},
                        %{"effort" => nil},
                        %{"unexpected" => "ignored"}
                      ],
                      "upgrade" => "bad\nupgrade"
                    }
                  ],
                  "nextCursor" => nil
                }}
             end)

    assert catalog.default_model == nil
    assert [entry] = catalog.models
    assert entry.default_reasoning_effort == nil
    assert entry.supported_reasoning_efforts == []
    assert entry.upgrade == nil
  end
end
