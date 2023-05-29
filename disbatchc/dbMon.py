#!/usr/bin/env python3

import curses, json, os, sys, time

from disbatchc.kvsstcp import KVSClient
from queue import Queue
from threading import Thread

# Connect to the disBatch communication service for this run.
try:
    kvscStatus = KVSClient(os.environ['DISBATCH_KVSSTCP_HOST'])
    kvscDisplay = kvscStatus.clone()
except:
    print('Cannot contact the disBatch server. This usally means the run has ended.', file=sys.stderr)
    sys.exit(1)

uniqueId = sys.argv[1]
uniqueIdName = os.path.split(uniqueId)[-1]

curses.initscr()
curses.start_color()
curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_RED)
curses.init_pair(4, curses.COLOR_YELLOW, curses.COLOR_BLACK)
curses.init_pair(5, curses.COLOR_RED, curses.COLOR_BLACK)
curses.init_pair(6, curses.COLOR_BLACK, curses.COLOR_BLACK)
curses.init_pair(7, curses.COLOR_WHITE, curses.COLOR_WHITE)
curses.curs_set(False)

CPCB, CPGB, CPBR, CPYB, CPRB, CPBB, CPWW = [curses.color_pair(x) for x in range(1, 8)]

Diamond = curses.ACS_DIAMOND
Horizontal, Vertical = curses.ACS_HLINE, curses.ACS_VLINE
CornerUL, CornerUR, CornerLL, CornerLR = curses.ACS_ULCORNER, curses.ACS_URCORNER, curses.ACS_LLCORNER, curses.ACS_LRCORNER
TeeD, TeeU, TeeR, TeeL = curses.ACS_TTEE, curses.ACS_BTEE, curses.ACS_LTEE, curses.ACS_RTEE

#TODO: Come up with a better way to set these based on the actual
#layout encoded in dbStatus.
HeaderLength = 6
FooterLength = 1
Width = 85

MinLines, MinCols = HeaderLength + FooterLength + 10, Width + 2

# Thread that periodically checks for status updates from the disBatch
# controller. Puts formatted results and auxillary data on the shared
# queue.
def dbStatus(kvsc, outq):
    while True:
        try:
            j = kvsc.view('DisBatch status')
        except:
            outq.put(('stop', None))
            break

        if j != b'<Starting...>':
            statusd = json.loads(j)

            now = time.time()

            # convert keys back to ints after json transform.
            engines = {int(k): v for k, v in statusd['engines'].items()}
            contexts = {int(k): v for k, v in statusd['contexts'].items()}
            ee = engines.values()
            statusd['slots'] = sum([e['active'] for e in ee if e['status'] == 'running'])
            statusd['finished'] = sum([e['finished'] for e in ee])
            statusd['failed'] = sum([e['failed'] for e in ee])
            header = []
            tuin = uniqueIdName if len(uniqueIdName) <= 40 else (uniqueIdName[:17] + '...' + uniqueIdName[-20:])
            label = f'Run label: {tuin:<40s}           Status: {statusd["more"]:15s}'
            header.append(([CornerUL] + [Horizontal]*Width + [CornerUR], CPCB))
            header.append(([Vertical] + [label + ' '*(Width - len(label))] + [Vertical], CPCB))
            header.append(([Vertical] + ['Slots {slots:5d}                  Tasks: Finished {finished:7d}      Failed{failed:5d}      Barrier{barriers:3d}'.format(**statusd)] + [Vertical], CPCB))
            header.append(([TeeR] + [Horizontal]*Width + [TeeL], CPCB))
            #                       '01234 012345678901 01234567890123456789 0123456  0123456 0123456789 0123456789 0123456'
            header.append(([Vertical] + ['Rank    Context           Host          Last     Avail   Assigned   Finished   Failed'] + [Vertical], CPCB))
            header.append(([CornerLL] + [Horizontal]*Width + [CornerLR], CPCB))
            assert len(header) == HeaderLength

            ee = sorted(engines.items())
            content = []
            for rank, engine in ee:
                if engine['status'] == 'stopped': continue
                engine['delay'] = now - engine['last']
                engine['cLabel'] = contexts[engine['cRank']]['label']
                content.append((rank, '{rank:5d} {cLabel:12.12s} {hostname:20.20s} {delay:6.0f}s {active:7d} {assigned:10d} {finished:10d} {failed:7d}'.format(**engine)))
            outq.put(('status', (engines, contexts, header, content)))
        time.sleep(3)

# Utility to pop up a Yes/No/Cancel dialog. Read reply from shared
# queue, return first acceptable response.
def popYNC(msg, parent, inq, title='Confirm'):
    ph, pw = parent.getmaxyx()
    h = int(ph * .75)
    w = int(pw * .85)
    ro, co = int((ph - h)*.5), int((pw - w)*.5)

    # Wrap msg to fit in pop up.
    l, msgw = '', []
    for word in msg.split():
        if len(word) > w:
            word = word[:w-3] + '...'
        if len(l) + 1 + len(word) > w:
            msgw.append(l)
            l = word
        else:
            l = l +  (' ' if l else '') + word
    msgw.append(l)
    if len(msgw) > h:
        missing = 1 + len(msgw) - h
        msgw = msgw[:h-1]
        msgw.append('%d lines elided.'%missing)

    nw = curses.newwin(h+2, w+2, ro, co)
    nw.border()
    nw.addstr(0, int((w - len(title))*.5), title)
    for r, l in enumerate(msgw):
        nw.addstr(r+1, 1, l)
    nw.addstr(r+2, int((w - 19)*.5), '[Y]es/[N]o/[C]ancel', curses.A_REVERSE)
    nw.refresh()

    # Acceptable responses. Treat a resize event as "cancel".
    resp = {ord('y'): 'Y', ord('Y'):  'Y', ord('n'): 'N', ord('N'): 'N', ord('c'): 'C', ord('C'): 'C', curses.KEY_RESIZE: 'C'}
    while True:
        tag, k = inq.get()
        if tag == 'key' and k in resp:
            break
        #TODO: If tag isn't key raise exception?
        
    parent.redrawwin()
    parent.refresh()
    return resp[k]

# Thread that paints the display and responds to user input. Reads status
# updates and keyboard input (including resize events) from the shared queue.
def display(S, kvsc, inq):
    content = []
    lenContent = len(content)

    header = [(' ', CPBB)]*4
    
    tooSmall = curses.LINES < MinLines or curses.COLS < MinCols
    displayLines = curses.LINES - (HeaderLength+FooterLength)

    localEngineStatus = {}

    contentCursor, contentFirst, done = 0, 0, False
    msg = ''
    while True:
        S.clear()

        if tooSmall:
            S.addstr(0, 0, 'Screen must be at least %dX%d'%(MinLines, MinCols), CPRB)
        else:
            # Header
            for r, (l, cp) in enumerate(header):
                S.move(r, 0)
                for e in l:
                    if type(e) is int:
                        S.addch(e, cp)
                    else:
                        S.addstr(e, cp)

            # Footer
            if msg or done:
                if done:
                    msg = '[disBatch controller has exited]' + (' ' if msg else '') + msg
                S.addstr(curses.LINES-1, 0, msg, CPBR)

            # Main content
            if content:
                # Adjust window to ensure cursor displays.
                if contentCursor < contentFirst:
                    # move window so last line corresponds to cursor, i.e.:
                    #    contentCursor == contentFirst + (displayLines-1)
                    contentFirst = max(0, contentCursor - (displayLines-1))
                elif contentCursor >= (contentFirst + displayLines):
                    # move window so first line corresponds to cursor.
                    contentFirst = contentCursor
                # ensure window is as full as possible.
                contentLast = min(contentFirst+displayLines, lenContent)
                contentFirst = max(0, contentLast-displayLines)
                for r, (rank, l) in enumerate(content[contentFirst:contentLast]):
                    if len(l) > curses.COLS-1:
                        l = l[:curses.COLS-4] + '...'
                    cp = CPGB
                    if engines[rank]['status'] == 'stopping':
                        cp = CPRB
                    elif localEngineStatus.get(rank, '') == 'requesting shutdown':
                        cp = CPYB
                    S.addstr(HeaderLength+r, 1, l, cp)

                # Scroll indicator and cursor
                regionStart = (displayLines * contentFirst)//lenContent
                regionEnd = (displayLines * contentLast + lenContent - 1)//lenContent
                S.addch(HeaderLength+regionStart, 0, TeeD, CPYB)
                for r in range(regionStart+1, regionEnd-1):
                    S.addch(HeaderLength+r, 0, Vertical, CPYB)
                S.addch(HeaderLength+regionEnd-1, 0, TeeU, CPYB)
                S.addch(HeaderLength+(contentCursor-contentFirst), 0, Diamond, CPCB)
            else:
                S.addstr(HeaderLength, 0, '<No Content>', CPRB)
                
        S.refresh()

        tag, o = inq.get()
        if tag == 'key':
            msg = ''
            k = o
            if k == curses.KEY_RESIZE:
                curses.update_lines_cols()
                if curses.LINES < MinLines or curses.COLS < MinCols:
                    tooSmall = True
                    continue
                tooSmall = False

                displayLines = curses.LINES - (HeaderLength+FooterLength)
                if displayLines > (lenContent - contentCursor):
                    contentFirst = max(0, lenContent - displayLines)
                else:
                    contentFirst = max(0, contentCursor - displayLines//2)

                S.clear()
                S.refresh()
                continue

            if   k == ord('u') or k == curses.KEY_UP:
                contentCursor = max(0, contentCursor-1)
            elif k == ord('d') or k == curses.KEY_DOWN:
                contentCursor = min(max(0, lenContent-1), contentCursor+1)
            elif k == ord('q'):
                break
            elif k in [ord('h'), ord('?')]:
                msg = 'C: Shutdown context; E: Shutdown engine; q: quit'
            elif k in [ord('C'), ord('E')]:
                if not done:
                    target = content[contentCursor][0]
                    if target is not None:
                        if k == ord('C'):
                            cRank = engines[target]['cRank']
                            r = popYNC('Stopping context {cLabel:s} ({cRank:d})'.format(**engines[target]), S, inq)
                            if r == 'Y':
                                try:
                                    msg = 'Asking controller to stop context %r'%cRank
                                    kvsc.put('.controller', ('stop context', cRank))
                                    for rank, e in engines.items():
                                        if e['cRank'] == cRank:
                                            localEngineStatus[rank] = 'requesting shutdown'
                                except socket.error:
                                    pass
                        elif k == ord('E'):
                            r = popYNC('Stopping engine {rank:d} ({hostname:s}, {pid:d})'.format(**engines[target]), S, inq)
                            if r == 'Y':
                                try:
                                    msg = 'Asking controller to stop engine  %r'%target
                                    kvsc.put('.controller', ('stop engine', target))
                                    localEngineStatus[target] = 'requesting shutdown'
                                except socket.error:
                                    pass
            else:
                msg = 'Got unrecognized key: %d'%k
        elif tag == 'status':
            engines, contexts, header, content = o
            # Adjust cursor location if needed.
            oldLen, lenContent = lenContent, len(content)
            if oldLen > lenContent:
                f = contentCursor/oldLen
                contentCursor = int(f*lenContent)
        elif tag == 'stop':
            done = True
        else:
            msg = 'Unrecognized tag: "%s",'%tag
            
# (Wrapped) main.
# Creates a shared queue, sets up status and display threads, and then waits for
# keyboard events and writes them to the shared queue. Intercepts "q" to quit.
#
# It appears that getch() needs to be called from the main processes.
def main(S):
    S.bkgdset(CPBB)
    S.clear()
    S.refresh()

    inq = Queue()
    gc = Thread(target=display, args=(S, kvscDisplay, inq))
    gc.daemon = True
    gc.start()
    db = Thread(target=dbStatus, args=(kvscStatus, inq))
    db.daemon = True
    db.start()

    while True:
        k = S.getch()
        if k == ord('q'):
            break
        inq.put(('key', k))

curses.wrapper(main)
