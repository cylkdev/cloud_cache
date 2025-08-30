defmodule CloudCache.Support.S3Sandbox do
  @moduledoc false

  @registry :up_s3_sandbox
  @state "state"
  @disabled "disabled_pids"
  @sleep 10
  @keys :unique

  @doc """
  Starts the sandbox.
  """
  def start_link do
    Registry.start_link(keys: @keys, name: @registry)
  end

  @doc """
  Returns the registered response function for `describe_object/3` in the
  context of the calling process.
  """
  def describe_object_response(bucket, object, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (bucket) -> ...",
        "fn (bucket, object) -> ...",
        "fn (bucket, object, opts) -> ..."
      ]

    func = find!(:describe_object, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(bucket)

      2 ->
        func.(bucket, object)

      3 ->
        func.(bucket, object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–4):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `pre_sign/3` in the
  context of the calling process.
  """
  def pre_sign_response(bucket, object, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (bucket) -> ...",
        "fn (bucket, object) -> ...",
        "fn (bucket, object, opts) -> ..."
      ]

    func = find!(:pre_sign, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(bucket)

      2 ->
        func.(bucket, object)

      3 ->
        func.(bucket, object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–4):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `list_parts/4` in the
  context of the calling process.
  """
  def list_parts_response(bucket, object, upload_id, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (bucket) -> ...",
        "fn (bucket, object) -> ...",
        "fn (bucket, object, upload_id) -> ...",
        "fn (bucket, object, upload_id, opts) -> ..."
      ]

    func = find!(:list_parts, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(bucket)

      2 ->
        func.(bucket, object)

      3 ->
        func.(bucket, object, upload_id)

      4 ->
        func.(bucket, object, upload_id, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–4):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `create_multipart_upload/3` in the
  context of the calling process.
  """
  def create_multipart_upload_response(bucket, object, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (bucket) -> ...",
        "fn (bucket, object) -> ...",
        "fn (bucket, object, opts) -> ..."
      ]

    func = find!(:create_multipart_upload, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(bucket)

      2 ->
        func.(bucket, object)

      3 ->
        func.(bucket, object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–3):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `pre_sign_part/5` in the
  context of the calling process.
  """
  def pre_sign_part_response(bucket, object, upload_id, part_number, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (bucket) -> ...",
        "fn (bucket, object) -> ...",
        "fn (bucket, object, upload_id) -> ...",
        "fn (bucket, object, upload_id, part_number) -> ...",
        "fn (bucket, object, upload_id, part_number, opts) -> ..."
      ]

    func = find!(:pre_sign_part, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(bucket)

      2 ->
        func.(bucket, object)

      3 ->
        func.(bucket, object, upload_id)

      4 ->
        func.(bucket, object, upload_id, part_number)

      5 ->
        func.(bucket, object, upload_id, part_number, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–5):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `abort_multipart_upload/4`
  in the context of the calling process.
  """
  def abort_multipart_upload_response(bucket, object, upload_id, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (bucket) -> ...",
        "fn (bucket, object) -> ...",
        "fn (bucket, object, upload_id) -> ...",
        "fn (bucket, object, upload_id, opts) -> ..."
      ]

    func = find!(:abort_multipart_upload, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(bucket)

      2 ->
        func.(bucket, object)

      3 ->
        func.(bucket, object, upload_id)

      4 ->
        func.(bucket, object, upload_id, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–4):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `complete_multipart_upload/5`
  in the context of the calling process.
  """
  def complete_multipart_upload_response(bucket, object, upload_id, parts, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (bucket) -> ...",
        "fn (bucket, object) -> ...",
        "fn (bucket, object, upload_id) -> ...",
        "fn (bucket, object, upload_id, parts) -> ...",
        "fn (bucket, object, upload_id, parts, opts) -> ..."
      ]

    func = find!(:complete_multipart_upload, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(bucket)

      2 ->
        func.(bucket, object)

      3 ->
        func.(bucket, object, upload_id)

      4 ->
        func.(bucket, object, upload_id, parts)

      5 ->
        func.(bucket, object, upload_id, parts, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–5):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `copy_part/8`
  in the context of the calling process.
  """
  def copy_part_response(
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        part_number,
        range,
        opts
      ) do
    doc_examples =
      [
        "fn -> ...",
        "fn (dest_bucket) -> ...",
        "fn (dest_bucket, dest_object) -> ...",
        "fn (dest_bucket, dest_object, src_bucket) -> ...",
        "fn (dest_bucket, dest_object, src_bucket, src_object) -> ...",
        "fn (dest_bucket, dest_object, src_bucket, src_object, upload_id) -> ...",
        "fn (dest_bucket, dest_object, src_bucket, src_object, upload_id, part_number) -> ...",
        "fn (dest_bucket, dest_object, src_bucket, src_object, upload_id, part_number, range) -> ...",
        "fn (dest_bucket, dest_object, src_bucket, src_object, upload_id, part_number, range, opts) -> ..."
      ]

    func =
      find!(:copy_part, dest_bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(dest_bucket)

      2 ->
        func.(dest_bucket, dest_object)

      3 ->
        func.(dest_bucket, dest_object, src_bucket)

      4 ->
        func.(dest_bucket, dest_object, src_bucket, src_object)

      5 ->
        func.(dest_bucket, dest_object, src_bucket, src_object, upload_id)

      6 ->
        func.(
          dest_bucket,
          dest_object,
          src_bucket,
          src_object,
          upload_id,
          part_number
        )

      7 ->
        func.(
          dest_bucket,
          dest_object,
          src_bucket,
          src_object,
          upload_id,
          part_number,
          range
        )

      8 ->
        func.(
          dest_bucket,
          dest_object,
          src_bucket,
          src_object,
          upload_id,
          part_number,
          range,
          opts
        )

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0–8):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Registers sandbox responses for use in tests.

  Call this function in your test `setup` block with a list of tuples.

  Each tuple has two elements:

    * the first element is either:
      - a **bucket name** as a string (exact match), or
      - a **regular expression** that must match the bucket name

    * the second element is a function that returns the mocked response

  ## Examples

      SharedUtils.Support.S3Sandbox.set_list_objects_responses([
        {"test-bucket", fn ->
          {:ok, [
            %{
              e_tag: "etag",
              key: "your-key",
              last_modified: ~U[2023-08-18 10:32:49Z],
              owner: nil,
              size: 11,
              storage_class: "STANDARD"
            }
          ]}
        end}
      ])
  """
  def set_pre_sign_responses(tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{:pre_sign, bucket}, func} end)
    |> then(&SandboxRegistry.register(@registry, @state, &1, @keys))
    |> then(fn
      :ok -> :ok
      {:error, :registry_not_started} -> raise_not_started!()
    end)

    Process.sleep(@sleep)
  end

  def set_list_parts_responses(tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{:list_parts, bucket}, func} end)
    |> then(&SandboxRegistry.register(@registry, @state, &1, @keys))
    |> then(fn
      :ok -> :ok
      {:error, :registry_not_started} -> raise_not_started!()
    end)

    Process.sleep(@sleep)
  end

  def set_create_multipart_upload_responses(tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{:create_multipart_upload, bucket}, func} end)
    |> then(&SandboxRegistry.register(@registry, @state, &1, @keys))
    |> then(fn
      :ok -> :ok
      {:error, :registry_not_started} -> raise_not_started!()
    end)

    Process.sleep(@sleep)
  end

  def set_pre_sign_part_responses(tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{:pre_sign_part, bucket}, func} end)
    |> then(&SandboxRegistry.register(@registry, @state, &1, @keys))
    |> then(fn
      :ok -> :ok
      {:error, :registry_not_started} -> raise_not_started!()
    end)

    Process.sleep(@sleep)
  end

  def set_abort_multipart_upload_responses(tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{:abort_multipart_upload, bucket}, func} end)
    |> then(&SandboxRegistry.register(@registry, @state, &1, @keys))
    |> then(fn
      :ok -> :ok
      {:error, :registry_not_started} -> raise_not_started!()
    end)

    Process.sleep(@sleep)
  end

  def set_complete_multipart_upload_responses(tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{:complete_multipart_upload, bucket}, func} end)
    |> then(&SandboxRegistry.register(@registry, @state, &1, @keys))
    |> then(fn
      :ok -> :ok
      {:error, :registry_not_started} -> raise_not_started!()
    end)

    Process.sleep(@sleep)
  end

  def set_copy_part_responses(tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{:copy_part, bucket}, func} end)
    |> then(&SandboxRegistry.register(@registry, @state, &1, @keys))
    |> then(fn
      :ok -> :ok
      {:error, :registry_not_started} -> raise_not_started!()
    end)

    Process.sleep(@sleep)
  end

  @doc """
  Disables the sandbox for the calling process.
  """
  @spec disable_s3_sandbox(map) :: :ok
  def disable_s3_sandbox(_context) do
    with {:error, :registry_not_started} <-
           SandboxRegistry.register(@registry, @disabled, %{}, @keys) do
      raise_not_started!()
    end
  end

  @doc """
  Returns true if the sandbox is disabled for the calling process.
  """
  @spec sandbox_disabled? :: boolean
  def sandbox_disabled? do
    case SandboxRegistry.lookup(@registry, @disabled) do
      {:ok, _} -> true
      {:error, :registry_not_started} -> raise_not_started!()
      {:error, :pid_not_registered} -> false
    end
  end

  @doc """
  Returns the registered response function for a given `action`
  and `bucket` pair, or raises an error message if the registry
  or handlers are not set up.

  `find!/3` looks up the current process (or its ancestor chain)
  in the sandbox registry and resolves the response function to
  call for the given `action` and `bucket`.

  ## Matching rules

  `find!/3` checks registered responses in this order:

    1. **Exact match:** when the bucket string matches exactly.

    2. **Regex match:** when the bucket string matches a
      registered regular expression.

  ## Returns

    * The **response function** to be invoked by the caller.

  ## Raises

    * `RuntimeError` with guidance if **no functions have been
      registered** for the calling PID.

    * `RuntimeError` with setup instructions if the **registry
      is not started**.

    * `RuntimeError` with a detailed diff if a **function is not
      found** for the given `action/bucket`.

    * `RuntimeError` if the **registered value has an unexpected
      format** (e.g., not a function).

  If nothing is registered for the calling PID, `find!/3` raises
  with a message that shows the available keys and an example of
  how to register responses for the given `action` and `bucket`.
  """
  @spec find!(action :: atom, bucket :: String.t(), doc_examples :: binary()) :: function
  def find!(action, bucket, doc_examples) do
    case SandboxRegistry.lookup(@registry, @state) do
      {:ok, state} ->
        find_response!(state, action, bucket, doc_examples)

      {:error, :pid_not_registered} ->
        raise """
        No functions have been registered for #{inspect(self())}.

        Action: #{inspect(action)}
        Bucket: #{inspect(bucket)}

        Add one of the following patterns to your test setup:

        #{format_example(action, bucket, doc_examples)}

        Replace `_response` with the value you want the sandbox to return.
        This determines how #{inspect(__MODULE__)} responds when
        `#{inspect(action)}` is called on bucket "#{bucket}".
        """

      {:error, :registry_not_started} ->
        raise """
        Registry not started for #{inspect(__MODULE__)}.

        Add the following line to your `test_helper.exs` to ensure the
        registry is started for this application:

            #{inspect(__MODULE__)}.start_link()
        """
    end
  end

  defp find_response!(state, action, bucket, doc_examples) do
    sandbox_key = {action, bucket}

    with state when is_map(state) <- Map.get(state, sandbox_key, state),
         regexes <-
           Enum.filter(state, fn {{_registered_action, registered_pattern}, _func} ->
             regex?(registered_pattern)
           end),
         {_action_pattern, func} when is_function(func) <-
           Enum.find(regexes, state, fn {{registered_action, regex}, _func} ->
             Regex.match?(regex, bucket) and registered_action === action
           end) do
      func
    else
      func when is_function(func) ->
        func

      functions when is_map(functions) ->
        functions_text =
          Enum.map_join(functions, "\n", fn {key, val} ->
            " #{inspect(key)} => #{inspect(val)}"
          end)

        example =
          action
          |> format_example(bucket, doc_examples)
          |> indent("  ")

        raise """
        Function not found.

          action: #{inspect(action)}
          bucket: #{inspect(bucket)}
          pid: #{inspect(self())}

        Found:

        #{functions_text}

        ---

        You need to register mock responses for `#{inspect(action)}` requests
        so the sandbox knows how to respond during tests.

        Add the following to your `test_helper.exs` or inside the test’s
        `setup` block:

        #{example}
        """

      other ->
        raise """
        Unrecognized input for #{inspect(sandbox_key)} in #{inspect(self())}.

        Response does not match the expected format for #{inspect(__MODULE__)}.

        Found value:

        #{inspect(other)}

        To fix this, update your test setup to include one of the following
        response patterns:

        #{format_example(action, bucket, doc_examples)}

        Replace `_response` with the value you want the sandbox to return.
        This determines how #{inspect(__MODULE__)} responds when
        `#{inspect(action)}` is called on bucket "#{bucket}".
        """
    end
  end

  defp regex?(%Regex{}), do: true
  defp regex?(_), do: false

  defp indent(text, prefix) do
    text
    |> String.split("\n", trim: false)
    |> Enum.map_join("\n", &"#{prefix}#{&1}")
  end

  defp format_example(action, _bucket, doc_examples) do
    """
    alias #{inspect(__MODULE__)}

    setup do
      S3Sandbox.set_#{action}_responses([
        #{Enum.map_join(doc_examples, "\n    # or\n", &("    " <> &1))}
        # or
        {~r|http://na1|, fn -> _response end}
      ])
    end
    """
  end

  defp raise_not_started! do
    raise """
    Registry not started for #{inspect(__MODULE__)}.

    To fix this, add the following line to your `test_helper.exs`:

        #{inspect(__MODULE__)}.start_link()

    This ensures the registry is running for your tests.
    """
  end
end
