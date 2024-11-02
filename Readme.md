# Protohackers

My solutions to a set of server programming challenges published here: https://protohackers.com/

## Spoilers Ahead

### 01 - Prime Time

The grader here seems to be very lax, according to the problem description we might get an input like `1.3e1` to which we'd obviously have to return `true`, but such an input never occurs.

### 02 - Means to an End

Sum is biiig

### 03 - Budget Chat

Be careful to not inform a user about events that happened after the user connected but before they fully joined.

### 04 - Unusual Database Program

Don't strip newlines!

### 05 - Mob in the Middle

Don't get confused by reading the messages sent by the tester. One message reads "7PBqhi52gBQukUuL4EUegLYQHW is too short", yet the address is valid and should be replaced.

### 08 - Inseccure Sockets Layer

Let me know if anyone finds a nice identity test for the cipher spec. I just ended up brute-forcing it.

### 09 - Job Centre

Serving 1000 clients at a time, on Linux, requires an increased open file limit. `ulimit -n` shows the current limit, `ulimit -n 10000` increases the limit. This increased limit is only valid within and for the duration of the current shell.

### 11 - Pest Control

`Resource temporarily unavailable (os error 11)` can be caused by `TcpStream::set_read_timeout`.

Make sure to catch not just too small lengths, but also too long.
