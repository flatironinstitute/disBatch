#!/usr/bin/python
import curses, json, os, socket, sys, time

import Queue
from threading import Thread

from kvsstcp import KVSClient

kvsc = KVSClient(sys.argv[1])
uniqueId = sys.argv[2]

def waitGetch(stdscr, q):
    while True:
        k = stdscr.getch()
        q.put(('key', k))

def dbStatus(q):
    while True:
        try:
            j = kvsc.view('DisBatch status')
            q.put(('status', j))
            time.sleep(1)
        except:
            q.put(('stop', None))
            break
        
def draw_menu(stdscr):
    # Clear and refresh the screen for a blank canvas
    stdscr.clear()
    stdscr.refresh()

    # Start colors in curses
    curses.start_color()
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_RED)
    curses.curs_set(False)
    
    q = Queue.Queue()
    gc = Thread(target=waitGetch, args=(stdscr, q))
    gc.daemon = True
    gc.start()
    db = Thread(target=dbStatus, args=(q,))
    db.daemon = True
    db.start()

    col, row, done, r2k, statusLine = 0, 0, False, {}, ''
    while (True):
        lRow, lCol = row, col
        tag, o = q.get()
        
        height, width = stdscr.getmaxyx()

        if tag == 'status':
            now = time.time()
            stdscr.clear()
            try:
                d = json.loads(o)
                open('dmon.txt', 'a').write('\n%r\n'%d)
                # convert keys back to ints after json transform.
                engines = dict([(int(k), v) for k, v in d['engines'].items()])
                contexts = dict([(int(k), v) for k, v in d['contexts'].items()])
                ee = engines.values()
                d['slots'] = sum([len(e['cylinders']) for e in ee if e['status'] == 'running'])
                d['finished'] = sum([e['finished'] for e in ee])
                d['failed'] = sum([e['failed'] for e in ee])
                stdscr.addstr(0, 0, uniqueId + (': {more:15s}  Run{finished:7d}  Failed{failed:7d}  Barriers{barriers:4d}  Total slots{slots:4d}'.format(**d)), curses.color_pair(1))
                #                       '01234 012345678901 01234567890123456789 0123456789 01234 012345678 0123456 0123456789 0123456789 0123456789'
                stdscr.addstr(2, 0,     ' Rank    Context           Host            PID      Age     Last    Avail   Assigned   Finished    Failed  ', curses.color_pair(1) | curses.A_UNDERLINE)
                r, r2k = 3, {}
                ee = sorted(engines.items())
                for rank, engine in ee:
                    if engine['status'] == 'stopped': continue
                    r2k[r] = rank
                    engine['slots'] = len(engine['cylinders'])
                    engine['delay'] = now - engine['last']
                    engine['cLabel'] = contexts[engine['cRank']]['uniqueId']
                    stdscr.addstr(r, 0, '{rank:5d} {cLabel:12.12s} {hostname:20.20s} {pid:10d} {age:5d} {delay:8.1f}s {slots:7d} {assigned:10d} {finished:10d} {failed:10d}'.format(**engine), curses.color_pair(2))
                    r += 1
            except ValueError:
                stdscr.addstr(0, 0, o, curses.color_pair(1))
        elif tag == 'key':
            statusLine = ''
            k = o
            if   k == ord('q'):
                break
            if   k == curses.KEY_DOWN:
                row = row + 1
            elif k == curses.KEY_UP:
                row = row - 1
            elif k == curses.KEY_RIGHT:
                col = col + 1
            elif k == curses.KEY_LEFT:
                col = col - 1
            elif k in [ord('C'), ord('E')]:
                if not done:
                    #TODO: Add confirmation.
                    target = r2k.get(row, 'No Man')
                    if k == ord('C'):
                        statusLine = 'Stopping context {cLabel:s} ({cRank:d})'.format(**engines[target]) + ' %d'%row
                        try:
                            kvsc.put('.controller', ('stop context', engines[target]['cRank']))
                        except socket.error:
                            pass
                    elif k == ord('E'):
                        statusLine = 'Stopping engine {rank:d} ({hostname:s}, {pid:d})'.format(**engines[target])
                        try:
                            kvsc.put('.controller', ('stop engine', target))
                        except socket.error:
                            pass
            else:
                statusLine = 'Last unrecognized key %d'%k
        elif tag == 'stop':
            done = True
            statusLine = 'Run ended.'
        else:
            raise Exception('Unknown tag: '+tag)

        col = max(0, col)
        col = min(width-1, col)

        row = max(3, row)
        row = min(height-1, row)

        stdscr.move(height-1, 0)
        stdscr.clrtoeol();
        stdscr.addstr(statusLine, curses.color_pair(3))

        stdscr.chgat(lRow, lCol, 1, curses.A_NORMAL)
        stdscr.chgat(row, col, 1, curses.A_REVERSE)

        # Refresh the screen
        stdscr.refresh()

def main():
    curses.wrapper(draw_menu)

if __name__ == "__main__":
    main()
