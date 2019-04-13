-- Buffer with timer-based retry capabilities
--
-- Usage:
--
--   local SendBuffer = require "kong.tools.send_buffer"
--
--   local send = function(entries)
--     ...
--     -- send the given array of entries somewhere (maybe via http, or writing in a file)
--     -- this function must return a truthy value if ok, or nil + error otherwise
--     return true
--   end
--
--   local buf = SendBuffer.new(
--     send, -- function used to "send/consume" values from the buffer
--     { -- Opts table with control values. Defaults shown:
--       retry_count   = 0,    -- number of times to retry sending
--       queue_size    = 1000, -- max number of entries that can be queued before they are queued for sending
--       send_delay    = 1,    -- in seconds, how often the payloads are grouped and queued for sending
--       flush_timeout = 2,    -- in seconds, how much time passes without activity before the current group is queued for sending
--       log           = ngx.log, -- or some other custom log function
--     }
--   )
--
--   buf:add("Some value")
--   buf:add("Some other value")
--
-- Given the example above,
--
-- * Calling `buf:flush()` manually would "close" the current_entries and queue them for sending as a group.
-- * Since send_delay is 1, the buffer will not try to send anything before 1 second passes
-- * Assuming both `add` methods are called within that second, a call to `send` will be scheduled with two entries: {"Some value", "Some other value"}.
-- * If sending fails, it will not be retried (retry_count equals 0)
-- * If retry_count was bigger than 0, sending would be re-queued n times before finally being discarded.
-- * The retries are not regular: every time a send fails, they next retry is delayed by n_try^2, up to 60s.
-- * The opts.log function will be used to log any errors
--
-- The most important internal attributes of Buffer are:
--
-- * `self.current_entries`: This is a list of objects which are added with buff:add()
-- * `self.entry_groups_queue`: This is an array of tables of type entry_group, which has the following structure:
--   { retries = 0,                                 -- how many times we have tried to send this node
--     entries = { "some data", "some other data" } -- array of entries to be sent
--   }
--
-- When you call `buf:add(entry)`, the entry is added to self.current_entries.
-- When you call `buf:flush()`, `self.current_entries` is "closed" and put on a queue for future sending. self.current_entries is reset to a new group. timers are set.

local setmetatable = setmetatable
local timer_at = ngx.timer.at
local remove = table.remove
local type = type
local huge = math.huge
local fmt = string.format
local min = math.min
local now = ngx.now
local ERR = ngx.ERR
local DEBUG = ngx.DEBUG
local WARN = ngx.WARN


-- max delay of 60s
local RETRY_MAX_DELAY = 60


local Buffer = {}


local Buffer_mt = {
  __index = Buffer
}


-- Forward function declarations
local flush
local send


-------------------------------------------------------------------------------
-- Create a timer for the `flush` operation.
-- @param self Buffer
local function schedule_flush(self)
  local ok, err = timer_at(self.flush_timeout/1000, flush, self)
  if not ok then
    self.log(ERR, "failed to create delayed flush timer: ", err)
    return
  end
  --log(DEBUG, "delayed timer created")
  self.flush_scheduled = true
end


-------------------------------------------------------------------------------
-- Create a timer for the `send` operation.
-- @param self Buffer
-- @param entry_group: table with `entries` and `retries` counter
-- @param delay number: timer delay in seconds
local function schedule_send(self, entry_group, delay)
  local ok, err = timer_at(delay, send, self, entry_group)
  if not ok then
    self.log(ERR, "failed to create send timer: ", err)
    return
  end
  self.send_scheduled = true
end

-----------------
-- Timer handlers
-----------------


-------------------------------------------------------------------------------
-- Get the current time.
-- @return current time in seconds
local function get_now()
  return now()*1000
end


-------------------------------------------------------------------------------
-- Timer callback for triggering a buffer flush.
-- @param premature boolean: ngx.timer premature indicator
-- @param self Buffer
-- @return nothing
flush = function(premature, self)
  if premature then
    return
  end

  if get_now() - self.last_t < self.flush_timeout then
    -- flushing reported: we had activity
    self.log(DEBUG, "[flush] buffer had activity, ",
               "delaying flush")
    schedule_flush(self)
    return
  end

  -- no activity and timeout reached
  self.log(DEBUG, "[flush] buffer had no activity, flushing ",
             "triggered by flush_timeout")
  self:flush()
  self.flush_scheduled = false
end


-------------------------------------------------------------------------------
-- Timer callback for issuing the `send` operation
-- @param premature boolean: ngx.timer premature indicator
-- @param self Buffer
-- @param entry_group: table with `entries` and `retries` counter
-- @return nothing
send = function(premature, self, entry_group)
  if premature then
    return
  end

  local next_retry_delay

  local ok, err = self.send(entry_group.entries)
  if ok then -- success, set retry_delays to 1
    self.retry_delay = 1
    next_retry_delay = 1

  else
    self.log(ERR, "failed to send entries: ", tostring(err))

    entry_group.retries = entry_group.retries + 1
    if entry_group.retries < self.retry_count then
      -- queue our data for sending again, at the end of the queue
      self.entry_groups_queue[#self.entry_groups_queue + 1] = entry_group
    else
      self.log(WARN, fmt("entry group was already tried %d times, dropping it",
                         entry_group.retries))
    end

    self.retry_delay = self.retry_delay + 1
    next_retry_delay = min(RETRY_MAX_DELAY, self.retry_delay * self.retry_delay)
  end

  if #self.entry_groups_queue > 0 then -- more to send?
    self.log(DEBUG, fmt("sending oldest data, %d still queued",
                        #self.entry_groups_queue - 1))
    local oldest_entry_group = remove(self.entry_groups_queue, 1)
    schedule_send(self, oldest_entry_group, next_retry_delay)
    return
  end

  -- we finished flushing the entry_groups_queue, allow the creation
  -- of a future timer once the current data reached its limit
  -- and we trigger a flush()
  self.send_scheduled = false
end


---------
-- Buffer
---------


-------------------------------------------------------------------------------
-- Initialize a generic log buffer.
-- @param send function, invoked to send every payload generated
-- @param opts table, optinally including
-- `retry_count`, `flush_timeout`, `queue_size`, `send_delay` and `log`
-- @return table: a Buffer object.
function Buffer.new(send, opts)
  opts = opts or {}

  assert(type(send) == "function",
         "arg #1 (send) must be a function")
  assert(type(opts) == "table",
         "arg #2 (opts) must be a table")
  assert(opts.retry_count == nil or type(opts.retry_count) == "number",
         "retry_count must be a number")
  assert(opts.flush_timeout == nil or type(opts.flush_timeout) == "number",
         "flush_timeout must be a number")
  assert(opts.queue_size == nil or type(opts.queue_size) == "number",
         "queue_size must be a number")
  assert(opts.send_delay == nil or type(opts.queue_size) == "number",
         "send_delay must be a number")
  assert(opts.log == nil or type(opts.log) == "function",
         "log must be a function")

  local self = {
    send = send,

    -- flush timeout in milliseconds
    flush_timeout = opts.flush_timeout and opts.flush_timeout * 1000 or 2000,
    retry_count = opts.retry_count or 0,
    queue_size = opts.queue_size or 1000,
    send_delay = opts.send_delay or 1,
    log = opts.log or ngx.log,

    retry_delay = 1,

    entry_groups_queue = {},
    current_entries = {},

    flush_scheduled = false,
    send_scheduled = false,

    last_t = huge,
  }

  return setmetatable(self, Buffer_mt)
end


-------------------------------------------------------------------------------
-- Add data to the buffer
-- @param payload included in the buffer
-- @return true, or nil and an error message.
function Buffer:add(entry)
  local new_size = #self.current_entries + 1
  self.current_entries[new_size] = entry

  if new_size >= self.queue_size then
    local ok, err = self:flush()
    if not ok then
      return nil, err
    end

  elseif not self.flush_scheduled then
    schedule_flush(self)
  end

  self.last_t = get_now()

  return true
end


-------------------------------------------------------------------------------
-- Enqueue the current entries as a group in the sending queue,
-- make self.entries empty and schedule sending with self.send if needed.
-- @return true, or nil and an error message.
function Buffer:flush()
  local count = #self.current_entries

  -- First we queue whichever entries we have so far
  if count > 0 then
    self.entry_groups_queue[#self.entry_groups_queue + 1] = {
      entries = self.current_entries,
      retries = 0,
    }

    self.current_entries = {}
    self.log(DEBUG, "queueing entries for sending (", count, " entries)")
  end

  -- Then, if there are items queued for sending, schedule a send
  -- in the future
  -- Only allow a single timer to send entries at a time
  -- this timer will keep calling itself while it has payloads
  if #self.entry_groups_queue > 0 and not self.send_scheduled then
    self.log(DEBUG, fmt("sending oldest entry, %d still queued",
                        #self.entry_groups_queue - 1))
    local oldest_entry_group = remove(self.entry_groups_queue, 1)
    schedule_send(self, oldest_entry_group, self.send_delay)
  end

  return true
end


return Buffer
