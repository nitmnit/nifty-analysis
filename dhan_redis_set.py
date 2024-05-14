import curses, time
import redis
import config

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

def input_char():
    try:
        win = curses.initscr()
        win.addstr(0, 0, '')
        while True:
            ch = win.getch()
            if ch in range(32, 127):
                break
            time.sleep(0.05)
    finally:
        curses.endwin()
    return chr(ch)


while True:
    # c = input_char()
    win = curses.initscr()
    c = chr(win.getch())
    if c == 'k':
        r.set(config.DIRECTION_REDIS_KEY, "down")
        print("setting up")
    elif c == 'j':
        r.set(config.DIRECTION_REDIS_KEY, "up")
        print("setting down")
    elif c == 'l':
        r.set(config.DIRECTION_REDIS_KEY, "stag")
        print("setting stag")
    time.sleep(0.05)
    win.addstr(0, 0, '')
