has_json? =
  try do
    :json.encode(1)
  rescue
    _ -> false
  else
    _ -> true
  end

exclude =
  if has_json? do
    []
  else
    [:json]
  end

ExUnit.start(exclude: exclude)
