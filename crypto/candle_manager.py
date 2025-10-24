import threading
import time

from crypto.api.binance import update_file

# To let binance update its 1-minute candle
DELAY_SECS = 1

class CandleManager:
    """Runs update_file() once per minute in a background thread."""

    def __init__(self, file_lock, call_back):
        self.file_lock = file_lock
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.call_back = call_back

    def start(self):
        print("Starting CandleManager thread...")
        self.thread.start()

    def stop(self):
        print("Stopping CandleManager thread...")
        self.stop_event.set()
        self.thread.join()

    def _run(self):
        while not self.stop_event.is_set():
            # sleep until the next minute boundary
            sleep_time = 60 - (time.time() % 60)
            self.stop_event.wait(timeout=sleep_time + DELAY_SECS)
            if self.stop_event.is_set():
                break
            print("Minute elapsed, updating candles...")
            self._update_file()
            self.call_back()

    def _update_file(self):
        try:
            with self.file_lock:
                update_file()
        except Exception as e:
            print(f"Error updating candle file: {e}")
