defmodule Kaffe.Config.ConsumerTest do
  use ExUnit.Case

  def change_config(subscriber_name, update_fn) do
    config = Application.get_env(:kaffe, :consumers)[subscriber_name]
    config = update_fn.(config)
    # Always use atom keys in keyword lists (Elixir standard)
    Application.put_env(:kaffe, :consumers, subscriber_name: config)
  end

  describe "configuration/1" do
    setup do
      change_config(:subscriber_name, fn config ->
        config
        |> Keyword.delete(:offset_reset_policy)
        |> Keyword.delete(:ssl)
        |> Keyword.put(:start_with_earliest_message, true)
      end)
    end

    test "correct settings are extracted" do
      sasl = Kaffe.Config.Consumer.config_get!(:subscriber_name, :sasl)

      change_config(:subscriber_name, fn config ->
        config |> Keyword.delete(:sasl)
      end)

      expected = %{
        endpoints: [{~c"kafka", 9092}],
        subscriber_name: :subscriber_name,
        consumer_group: "kaffe-test-group",
        topics: ["kaffe-test"],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 10
        ],
        consumer_config: [
          auto_start_producers: false,
          allow_topic_auto_creation: false,
          begin_offset: :earliest
        ],
        message_handler: SilentMessage,
        async_message_ack: false,
        rebalance_delay_ms: 100,
        max_bytes: 10_000,
        min_bytes: 0,
        max_wait_time: 10_000,
        subscriber_retries: 1,
        subscriber_retry_delay_ms: 5,
        offset_reset_policy: :reset_by_subscriber,
        worker_allocation_strategy: :worker_per_partition,
        client_down_retry_expire: 15_000
      }

      on_exit(fn ->
        change_config(:subscriber_name, fn config ->
          Keyword.put(config, :sasl, sasl)
        end)
      end)

      assert Kaffe.Config.Consumer.configuration(:subscriber_name) == expected
    end

    test "string endpoints parsed correctly" do
      endpoints = Kaffe.Config.Consumer.config_get!(:subscriber_name, :endpoints)

      change_config(:subscriber_name, fn config ->
        config |> Keyword.put(:endpoints, "kafka:9092,localhost:9092")
      end)

      expected = %{
        endpoints: [{~c"kafka", 9092}, {~c"localhost", 9092}],
        subscriber_name: :subscriber_name,
        consumer_group: "kaffe-test-group",
        topics: ["kaffe-test"],
        group_config: [
          offset_commit_policy: :commit_to_kafka_v2,
          offset_commit_interval_seconds: 10
        ],
        consumer_config: [
          auto_start_producers: false,
          allow_topic_auto_creation: false,
          begin_offset: :earliest
        ],
        message_handler: SilentMessage,
        async_message_ack: false,
        rebalance_delay_ms: 100,
        max_bytes: 10_000,
        min_bytes: 0,
        max_wait_time: 10_000,
        subscriber_retries: 1,
        subscriber_retry_delay_ms: 5,
        offset_reset_policy: :reset_by_subscriber,
        worker_allocation_strategy: :worker_per_partition,
        client_down_retry_expire: 15_000
      }

      on_exit(fn ->
        change_config(:subscriber_name, fn config ->
          Keyword.put(config, :endpoints, endpoints)
        end)
      end)

      assert Kaffe.Config.Consumer.configuration(:subscriber_name) == expected
    end
  end

  test "correct settings with sasl plain are extracted" do
    sasl = Kaffe.Config.Consumer.config_get!(:subscriber_name, :sasl)

    change_config(:subscriber_name, fn config ->
      Keyword.put(config, :sasl, %{mechanism: :plain, login: "Alice", password: "ecilA"})
    end)

    expected = %{
      endpoints: [{~c"kafka", 9092}],
      subscriber_name: :subscriber_name,
      consumer_group: "kaffe-test-group",
      topics: ["kaffe-test"],
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 10
      ],
      consumer_config: [
        auto_start_producers: false,
        allow_topic_auto_creation: false,
        begin_offset: :earliest,
        sasl: {:plain, "Alice", "ecilA"}
      ],
      message_handler: SilentMessage,
      async_message_ack: false,
      rebalance_delay_ms: 100,
      max_bytes: 10_000,
      min_bytes: 0,
      max_wait_time: 10_000,
      subscriber_retries: 1,
      subscriber_retry_delay_ms: 5,
      offset_reset_policy: :reset_by_subscriber,
      worker_allocation_strategy: :worker_per_partition,
      client_down_retry_expire: 15_000
    }

    on_exit(fn ->
      change_config(:subscriber_name, fn config ->
        Keyword.put(config, :sasl, sasl)
      end)
    end)

    assert Kaffe.Config.Consumer.configuration(:subscriber_name) == expected
  end

  test "correct settings with ssl are extracted" do
    ssl = Kaffe.Config.Consumer.config_get(:subscriber_name, :ssl, false)

    change_config(:subscriber_name, fn config ->
      Keyword.put(config, :ssl, true)
    end)

    expected = %{
      endpoints: [{~c"kafka", 9092}],
      subscriber_name: :subscriber_name,
      consumer_group: "kaffe-test-group",
      topics: ["kaffe-test"],
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 10
      ],
      consumer_config: [
        auto_start_producers: false,
        allow_topic_auto_creation: false,
        begin_offset: :earliest,
        ssl: true
      ],
      message_handler: SilentMessage,
      async_message_ack: false,
      rebalance_delay_ms: 100,
      max_bytes: 10_000,
      min_bytes: 0,
      max_wait_time: 10_000,
      subscriber_retries: 1,
      subscriber_retry_delay_ms: 5,
      offset_reset_policy: :reset_by_subscriber,
      worker_allocation_strategy: :worker_per_partition,
      client_down_retry_expire: 15_000
    }

    on_exit(fn ->
      change_config(:subscriber_name, fn config ->
        Keyword.put(config, :ssl, ssl)
      end)
    end)

    assert Kaffe.Config.Consumer.configuration(:subscriber_name) == expected
  end

  describe "offset_reset_policy" do
    test "computes correctly from start_with_earliest_message == true" do
      change_config(:subscriber_name, fn config ->
        config |> Keyword.delete(:offset_reset_policy)
      end)

      assert Kaffe.Config.Consumer.configuration(:subscriber_name).offset_reset_policy == :reset_by_subscriber
    end
  end

  describe "backward compatibility with legacy :consumer config" do
    test "falls back to :consumer config when subscriber not found in :consumers" do
      original_consumers = Application.get_env(:kaffe, :consumers)
      original_consumer = Application.get_env(:kaffe, :consumer)

      # Set up legacy :consumer config (pre-8f5034e format)
      Application.put_env(:kaffe, :consumer,
        endpoints: [kafka: 9092],
        topics: ["legacy-topic"],
        consumer_group: "legacy-group",
        message_handler: SilentMessage,
        offset_commit_interval_seconds: 15
      )

      # Set up :consumers with a different subscriber
      Application.put_env(:kaffe, :consumers,
        other_subscriber: [
          topics: ["other-topic"]
        ]
      )

      # Request config for a subscriber that doesn't exist in :consumers
      # Should fall back to merging with :consumer config
      config = Kaffe.Config.Consumer.consumer_config(:missing_subscriber)

      assert config[:topics] == ["legacy-topic"]
      assert config[:consumer_group] == "legacy-group"
      assert config[:message_handler] == SilentMessage
      assert config[:offset_commit_interval_seconds] == 15

      # Restore original config
      Application.put_env(:kaffe, :consumers, original_consumers)

      if original_consumer do
        Application.put_env(:kaffe, :consumer, original_consumer)
      else
        Application.delete_env(:kaffe, :consumer)
      end
    end

    test "merges :consumer config with :consumers keyword list config" do
      original_consumers = Application.get_env(:kaffe, :consumers)
      original_consumer = Application.get_env(:kaffe, :consumer)

      # Set up legacy :consumer config as base
      Application.put_env(:kaffe, :consumer,
        endpoints: [kafka: 9092],
        topics: ["base-topic"],
        consumer_group: "base-group",
        message_handler: SilentMessage,
        max_bytes: 50_000
      )

      # Set up :consumers with subscriber-specific overrides
      Application.put_env(:kaffe, :consumers,
        test_subscriber: [
          # This should override base
          topics: ["override-topic"],
          # This should override base
          consumer_group: "override-group",
          # This should be added to base
          max_wait_time: 5_000
          # endpoints, message_handler, max_bytes should come from base
        ]
      )

      config = Kaffe.Config.Consumer.consumer_config(:test_subscriber)

      # Should have overridden values
      assert config[:topics] == ["override-topic"]
      assert config[:consumer_group] == "override-group"
      assert config[:max_wait_time] == 5_000

      # Should have base values where not overridden
      assert config[:endpoints] == [kafka: 9092]
      assert config[:message_handler] == SilentMessage
      assert config[:max_bytes] == 50_000

      # Restore original config
      Application.put_env(:kaffe, :consumers, original_consumers)

      if original_consumer do
        Application.put_env(:kaffe, :consumer, original_consumer)
      else
        Application.delete_env(:kaffe, :consumer)
      end
    end

    test "works without :consumer config when subscriber exists in :consumers" do
      original_consumers = Application.get_env(:kaffe, :consumers)
      original_consumer = Application.get_env(:kaffe, :consumer)

      # Remove :consumer config entirely
      Application.delete_env(:kaffe, :consumer)

      # Set up only :consumers
      Application.put_env(:kaffe, :consumers,
        test_subscriber: [
          topics: ["test-topic"],
          consumer_group: "test-group"
        ]
      )

      config = Kaffe.Config.Consumer.consumer_config(:test_subscriber)

      assert config[:topics] == ["test-topic"]
      assert config[:consumer_group] == "test-group"

      # Restore original config
      Application.put_env(:kaffe, :consumers, original_consumers)

      if original_consumer do
        Application.put_env(:kaffe, :consumer, original_consumer)
      else
        Application.delete_env(:kaffe, :consumer)
      end
    end
  end
end
