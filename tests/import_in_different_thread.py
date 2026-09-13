"""Import the package and read one PV from a non-main thread's event loop."""

import asyncio
import sys
import threading

from repics.aio import caget

if __name__ == "__main__":

    async def get_value():
        print(await caget(sys.argv[1], timeout=5.0))

    t = threading.Thread(target=asyncio.new_event_loop().run_until_complete, args=[get_value()])
    t.start()
    t.join()
