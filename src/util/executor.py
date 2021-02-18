import subprocess
import os
import threading
from util.lock import Synchronized

class Executor(Synchronized):
    '''
    Object to run subprocess commands in a separate thread.
    This way, Python can continue operating while interacting 
    with subprocesses.
    '''
    def __init__(self, cmd, **kwargs):
        self.cmd = cmd
        self.started = False
        self.stopped = False
        self.thread = None
        self.process = None
        self.kwargs = kwargs

    # Run our command. Returns immediately after booting a thread
    def run(self):
        if self.started:
            raise RuntimeError('Executor already started. Make a new Executor for a new run')
        if self.stopped:
            raise RuntimeError('Executor already stopped. Make a new Executor for a new run')
        if self.kwargs == None:
            self.kwargs = kwargs

        def target(**kwargs):
            self.process = subprocess.Popen(self.cmd, **kwargs)
            self.process.communicate()
            self.stopped = True

        self.thread = threading.Thread(target=target, kwargs=self.kwargs)
        self.thread.start()
        self.started = True

    # Run our command on this thread, waiting until it completes.
    # Note: Some commands never return, be careful with this method!
    def run_direct(self):
        self.process = subprocess.Popen(self.cmd, **self.kwargs)
        self.started = True
        self.process.communicate()
        self.stopped = True
        return self.process.returncode

    # Block until this executor is done
    def wait(self):
        if not self.started:
            raise RuntimeError('Executor with command "{}" not yet started, cannot wait'.format(self.cmd))
        if self.stopped:
            return self.process.returncode
        self.thread.join()
        return self.process.returncode

    # Force-stop executor, wait until done
    def stop(self):
        if self.started and not self.stopped:
            if self.thread.is_alive():
                #If command fails, or when stopping directly after starting
                for x in range(5):
                    if self.process == None:
                        time.sleep(1)
                    else:
                        break
                if self.process != None:
                    self.process.terminate()
                self.thread.join()
                self.stopped = True
        return self.process.returncode if self.process != None else 1

    # Stop and then start wrapped command again
    # Note: This kills current thread, creates new one
    def reboot(self):
        self.stop()
        self.started = False
        self.stopped = False
        self.run(**self.kwargs)

    # Returns pid of running process, or -1 if it cannot access current process 
    def get_pid(self):
        if (not self.started) or self.stopped or self.process == None:
            return -1
        return self.process.pid

    #  Function to run all given executors, with same arguments
    @staticmethod
    def run_all(executors):
        for x in executors:
            x.run()

    '''
    Function to wait for all executors.
    If stop_on_error is True, we immediately kill all remaining executors.
    If return_returncodes is True, we return the process returncodes instead of regular True/False.
    Returns True if all processes sucessfully executed, False otherwise.
    '''
    @staticmethod
    def wait_all(executors, stop_on_error=True, return_returncodes=False):
        returncodes = []
        status = True
        for x in executors:
            returncode = x.wait()
            returncodes.append(returncode)
            if returncode != 0:
                if stop_on_error: # We had an error during execution and must stop all now
                    Executor.stop_all(executors) # Stop all other executors
                    return returncodes if return_returncodes else False
                else:
                    status = False
        return returncodes if return_returncodes else status

    # Function to stop all given execuors.
    # If as_generator is True, we return exit status codes as a generator  
    @staticmethod
    def stop_all(executors, as_generator=False):
        for x in executors:
            if as_generator:
                yield x.stop()
            else:
                x.stop()
