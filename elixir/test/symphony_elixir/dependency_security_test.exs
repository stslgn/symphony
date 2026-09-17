defmodule SymphonyElixir.DependencySecurityTest do
  use ExUnit.Case, async: true

  alias Mix.Dep.Lock

  # Floors cover the reviewed EEF advisories listed in DEPENDENCY_SECURITY.md.
  # This regression guard supplements, but does not replace, a fresh advisory scan.
  for {package, floor} <- [
        bandit: "1.12.5",
        decimal: "3.0.0",
        hpax: "1.0.4",
        mint: "1.10.0",
        phoenix: "1.8.9",
        phoenix_live_view: "1.1.33",
        plug: "1.19.5",
        req: "0.6.1"
      ] do
    @package package
    @floor floor

    test "#{package} stays at or above its reviewed security floor" do
      lock = Lock.read()
      assert {:hex, @package, version, _, _, _, _, _} = Map.fetch!(lock, @package)
      assert Version.compare(version, @floor) in [:eq, :gt]
    end
  end
end
