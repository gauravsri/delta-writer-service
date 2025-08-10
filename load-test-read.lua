-- Load test script for user read operations
-- Usage: wrk -t8 -c20 -d30s -s load-test-read.lua http://localhost:8080/api/v1/users

local counter = 0
local user_ids = {}

-- Pre-populate some user IDs to test against
init = function(args)
   for i = 1, 100 do
      user_ids[i] = "test_user_" .. i
   end
   print("Initialized with " .. #user_ids .. " test user IDs")
end

request = function()
   counter = counter + 1
   
   -- Alternate between specific user reads and partition queries
   if counter % 3 == 0 then
      -- Test partition queries
      return wrk.format("GET", "/api/v1/users?country=US", {})
   elseif counter % 3 == 1 then
      -- Test individual user reads
      local user_id = user_ids[math.random(1, #user_ids)]
      return wrk.format("GET", "/api/v1/users/" .. user_id, {})
   else
      -- Test date-based partition queries
      return wrk.format("GET", "/api/v1/users?signup_date=2024-01-01", {})
   end
end

response = function(status, headers, body)
   if status ~= 200 and status ~= 404 then
      print("Error: Status " .. status .. " Body: " .. body)
   end
end

done = function(summary, latency, requests)
   io.write("---------------------------------------\n")
   io.write("READ OPERATIONS LOAD TEST RESULTS\n")
   io.write("---------------------------------------\n")
   io.write(string.format("  Requests: %d\n", summary.requests))
   io.write(string.format("  Duration: %.2fs\n", summary.duration / 1000000))
   io.write(string.format("  Req/Sec:  %.2f\n", summary.requests / (summary.duration / 1000000)))
   io.write(string.format("  Errors:   %d\n", summary.errors.connect + summary.errors.read + summary.errors.write + summary.errors.timeout))
   io.write(string.format("  Min:      %.2fms\n", latency.min / 1000))
   io.write(string.format("  Mean:     %.2fms\n", latency.mean / 1000))
   io.write(string.format("  Max:      %.2fms\n", latency.max / 1000))
   io.write(string.format("  99th:     %.2fms\n", latency["99"] / 1000))
end