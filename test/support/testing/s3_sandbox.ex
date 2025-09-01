defmodule CloudCache.Testing.S3Sandbox do
  @moduledoc false

  @registry :cloud_cache_s3_sandbox
  @state "state"
  @disabled "disabled"
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
        "fn (object) -> ...",
        "fn (object, options) -> ..."
      ]

    func = find!(:describe_object, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `copy_object/5` in the
  context of the calling process.
  """
  def copy_object_response(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (dest_object) -> ...",
        "fn (dest_object, src_bucket) -> ...",
        "fn (dest_object, src_bucket, src_object) -> ...",
        "fn (dest_object, src_bucket, src_object, options) -> ..."
      ]

    func = find!(:copy_object, dest_bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(dest_object)

      2 ->
        func.(dest_object, src_bucket)

      3 ->
        func.(dest_object, src_bucket, src_object)

      4 ->
        func.(dest_object, src_bucket, src_object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
        "fn (object) -> ...",
        "fn (object, options) -> ..."
      ]

    func = find!(:pre_sign, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
        "fn (object) -> ...",
        "fn (object, upload_id) -> ...",
        "fn (object, upload_id, options) -> ..."
      ]

    func = find!(:list_parts, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, upload_id)

      3 ->
        func.(object, upload_id, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `upload_part/6`
  in the context of the calling process.
  """
  def upload_part_response(
        bucket,
        object,
        upload_id,
        part_number,
        body,
        opts
      ) do
    doc_examples =
      [
        "fn -> ...",
        "fn (object) -> ...",
        "fn (object, upload_id) -> ...",
        "fn (object, upload_id, part_number) -> ...",
        "fn (object, upload_id, part_number, body) -> ...",
        "fn (object, upload_id, part_number, body, options) -> ..."
      ]

    func = find!(:upload_part, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, upload_id)

      3 ->
        func.(object, upload_id, part_number)

      4 ->
        func.(object, upload_id, part_number, body)

      5 ->
        func.(object, upload_id, part_number, body, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
        "fn (object) -> ...",
        "fn (object, upload_id) -> ...",
        "fn (object, upload_id, part_number) -> ...",
        "fn (object, upload_id, part_number, options) -> ..."
      ]

    func = find!(:pre_sign_part, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, upload_id)

      3 ->
        func.(object, upload_id, part_number)

      4 ->
        func.(object, upload_id, part_number, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `copy_object_multipart/5` in the
  context of the calling process.
  """
  def copy_object_multipart_response(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    doc_examples =
      [
        "fn -> ...",
        "fn (dest_object) -> ...",
        "fn (dest_object, src_bucket) -> ...",
        "fn (dest_object, src_bucket, src_object) -> ..."
      ]

    func = find!(:copy_object_multipart, dest_bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(dest_object)

      2 ->
        func.(dest_object, src_bucket)

      3 ->
        func.(dest_object, src_bucket, src_object)

      4 ->
        func.(dest_object, src_bucket, src_object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

        #{Enum.map_join(doc_examples, "\n", &("    " <> &1))}
        """
    end
  end

  @doc """
  Returns the registered response function for `copy_parts/7`
  in the context of the calling process.
  """
  def copy_parts_response(
        dest_bucket,
        dest_object,
        src_bucket,
        src_object,
        upload_id,
        content_length,
        opts
      ) do
    doc_examples =
      [
        "fn -> ...",
        "fn (dest_object) -> ...",
        "fn (dest_object, src_bucket) -> ...",
        "fn (dest_object, src_bucket, src_object) -> ...",
        "fn (dest_object, src_bucket, src_object, upload_id) -> ...",
        "fn (dest_object, src_bucket, src_object, upload_id, content_length) -> ...",
        "fn (dest_object, src_bucket, src_object, upload_id, content_length, options) -> ..."
      ]

    func = find!(:copy_parts, dest_bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(dest_object)

      2 ->
        func.(dest_object, src_bucket)

      3 ->
        func.(dest_object, src_bucket, src_object)

      4 ->
        func.(dest_object, src_bucket, src_object, upload_id)

      5 ->
        func.(dest_object, src_bucket, src_object, upload_id, content_length)

      6 ->
        func.(
          dest_object,
          src_bucket,
          src_object,
          upload_id,
          content_length,
          opts
        )

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
        "fn (dest_object) -> ...",
        "fn (dest_object, src_bucket) -> ...",
        "fn (dest_object, src_bucket, src_object) -> ...",
        "fn (dest_object, src_bucket, src_object, upload_id) -> ...",
        "fn (dest_object, src_bucket, src_object, upload_id, part_number) -> ...",
        "fn (dest_object, src_bucket, src_object, upload_id, part_number, range) -> ...",
        "fn (dest_object, src_bucket, src_object, upload_id, part_number, range, options) -> ..."
      ]

    func = find!(:copy_part, dest_bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(dest_object)

      2 ->
        func.(dest_object, src_bucket)

      3 ->
        func.(dest_object, src_bucket, src_object)

      4 ->
        func.(dest_object, src_bucket, src_object, upload_id)

      5 ->
        func.(
          dest_object,
          src_bucket,
          src_object,
          upload_id,
          part_number
        )

      6 ->
        func.(
          dest_object,
          src_bucket,
          src_object,
          upload_id,
          part_number,
          range
        )

      7 ->
        func.(
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

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
        "fn (object) -> ...",
        "fn (object, upload_id) -> ...",
        "fn (object, upload_id, parts) -> ...",
        "fn (object, upload_id, parts, options) -> ..."
      ]

    func = find!(:complete_multipart_upload, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, upload_id)

      3 ->
        func.(object, upload_id, parts)

      4 ->
        func.(object, upload_id, parts, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
        "fn (object) -> ...",
        "fn (object, upload_id) -> ...",
        "fn (object, upload_id, options) -> ..."
      ]

    func = find!(:abort_multipart_upload, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, upload_id)

      3 ->
        func.(object, upload_id, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
        "fn (object) -> ...",
        "fn (object, options) -> ..."
      ]

    func = find!(:create_multipart_upload, bucket, doc_examples)

    case :erlang.fun_info(func)[:arity] do
      0 ->
        func.()

      1 ->
        func.(object)

      2 ->
        func.(object, opts)

      _ ->
        raise """
        This function's signature is not supported: #{inspect(func)}

        Please provide a function with one of the following arities (0-#{length(doc_examples) - 1}):

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
  def set_describe_object_responses(tuples) do
    set_responses(:describe_object, tuples)
  end

  def set_copy_object_responses(tuples) do
    set_responses(:copy_object, tuples)
  end

  def set_pre_sign_responses(tuples) do
    set_responses(:pre_sign, tuples)
  end

  def set_list_parts_responses(tuples) do
    set_responses(:list_parts, tuples)
  end

  def set_upload_part_responses(tuples) do
    set_responses(:upload_part, tuples)
  end

  def set_pre_sign_part_responses(tuples) do
    set_responses(:pre_sign_part, tuples)
  end

  def set_copy_object_multipart_responses(tuples) do
    set_responses(:copy_object_multipart, tuples)
  end

  def set_copy_parts_responses(tuples) do
    set_responses(:copy_parts, tuples)
  end

  def set_copy_part_responses(tuples) do
    set_responses(:copy_part, tuples)
  end

  def set_complete_multipart_upload_responses(tuples) do
    set_responses(:complete_multipart_upload, tuples)
  end

  def set_abort_multipart_upload_responses(tuples) do
    set_responses(:abort_multipart_upload, tuples)
  end

  def set_create_multipart_upload_responses(tuples) do
    set_responses(:create_multipart_upload, tuples)
  end

  defp set_responses(key, tuples) do
    tuples
    |> Map.new(fn {bucket, func} -> {{key, bucket}, func} end)
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
  def find!(action, bucket, doc_examples) do
    case SandboxRegistry.lookup(@registry, @state) do
      {:ok, state} ->
        find_response!(state, action, bucket, doc_examples)

      {:error, :pid_not_registered} ->
        raise """
        No functions have been registered for #{inspect(self())}.

        Action: #{inspect(action)}
        Adapter: #{inspect(bucket)}

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
