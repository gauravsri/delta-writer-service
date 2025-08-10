-- Load test script for batch user creation
-- Usage: wrk -t4 -c5 -d60s -s load-test-batch.lua http://localhost:8080/api/v1/users/batch

local counter = 0

request = function()
   counter = counter + 1
   local batch_size = 50  -- Test with 50 users per batch
   local users = {}
   
   for i = 1, batch_size do
      local user_id = "batch_test_" .. counter .. "_" .. i .. "_" .. os.time()
      users[i] = string.format([[{
         "user_id": "%s",
         "username": "batchuser%d_%d",
         "email": "batch%d_%d@example.com", 
         "country": "US",
         "signup_date": "2024-01-01"
      }]], user_id, counter, i, counter, i)
   end
   
   local body = string.format([[{
      "users": [%s]
   }]], table.concat(users, ","))
   
   return wrk.format("POST", nil, {
      ["Content-Type"] = "application/json"
   }, body)
end

response = function(status, headers, body)
   if status ~= 201 and status ~= 206 then
      print("Error: Status " .. status .. " Body: " .. body)
   end
end

done = function(summary, latency, requests)
   io.write("---------------------------------------\n")
   io.write("BATCH USER CREATION LOAD TEST RESULTS\n")
   io.write("---------------------------------------\n")
   io.write(string.format("  Requests: %d\n", summary.requests))
   io.write(string.format("  Duration: %.2fs\n", summary.duration / 1000000))
   io.write(string.format("  Req/Sec:  %.2f\n", summary.requests / (summary.duration / 1000000)))
   io.write(string.format("  Users Created: ~%d (50 per request)\n", summary.requests * 50))
   io.write(string.format("  Errors:   %d\n", summary.errors.connect + summary.errors.read + summary.errors.write + summary.errors.timeout))
   io.write(string.format("  Min:      %.2fms\n", latency.min / 1000))
   io.write(string.format("  Mean:     %.2fms\n", latency.mean / 1000))
   io.write(string.format("  Max:      %.2fms\n", latency.max / 1000))
   io.write(string.format("  99th:     %.2fms\n", latency["99"] / 1000))
end