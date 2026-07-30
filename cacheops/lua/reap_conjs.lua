local conj_key = KEYS[1]
local found = 0
local removed = 0

for index = 2, #KEYS do
    if redis.call('exists', KEYS[index]) == 0 then
        found = found + 1
        removed = removed + redis.call('srem', conj_key, KEYS[index])
    end
end

return {found, removed}
