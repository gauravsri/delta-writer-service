-- Load test script for single user creation
-- Usage: wrk -t4 -c10 -d30s -s load-test-single-user.lua http://localhost:8080/api/v1/users

local counter = 0

request = function()
   counter = counter + 1
   local user = {
      user_id = "load_test_user_" .. counter .. "_" .. os.time(),
      username = "loaduser" .. counter,
      email = "loadtest" .. counter .. "@example.com",
      country = "US",
      signup_date = "2024-01-01"
   }
   
   local body = string.format([[{
      "user_id": "%s",
      "username": "%s",
      "email": "%s",
      "country": "%s",
      "signup_date": "%s"
   }]], user.user_id, user.username, user.email, user.country, user.signup_date)
   
   return wrk.format("POST", nil, {
      ["Content-Type"] = "application/json"
   }, body)
end

response = function(status, headers, body)
   if status ~= 201 then
      print("Error: Status " .. status .. " Body: " .. body)
   end
end

done = function(summary, latency, requests)
   io.write("---------------------------------------\n")
   io.write("SINGLE USER CREATION LOAD TEST RESULTS\n")
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