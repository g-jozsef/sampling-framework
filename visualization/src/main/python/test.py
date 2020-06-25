import os
import re
import json
import matplotlib.pyplot as plt
import sys


rootdir = sys.argv[1]
freq = []
print(rootdir)
for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        f = os.path.join(subdir, file)
        z = re.match(r""".*[\\]([0-9]+)_output.*winner.*\.json""", f)
        if z:
            print(f)
            with open(f) as json_file:
                data = json.load(json_file)
                print(data[0]['fitness'])
                print(z.groups())
                time = 0
                memory = 0
                for t in data[0]['times']:
                    time = time + t
                for m in data[0]['counters']:
                    memory = memory + m
                time = time / len(data[0]['times'])
                memory = memory / len(data[0]['counters'])
                freq.append((int(z.groups()[0]), data[0]['fitness'], time, memory))


print(freq)
freq = sorted(freq)
print(freq)


fig, axs = plt.subplots(3, 1, constrained_layout=True)
axs[0].plot([x for y, x, x1, x2 in freq])
axs[0].set_ylabel('fitness')
axs[0].set_title('Fitness of optimizer over time')
axs[0].set_xlabel('iteration')

axs[1].plot([x1 for y, x, x1, x2 in freq])
axs[1].set_ylabel('time (ms)')
axs[1].set_title('Average time taken by winner')
axs[1].set_xlabel('iteration')


axs[2].plot([x2 for y, x, x1, x2 in freq])
axs[2].set_ylabel('counters (#)')
axs[2].set_title('Number of average counters used by winner')
axs[2].set_xlabel('iteration')

fig.suptitle('Optimization of algorithm over iterations', fontsize=16)

plt.show()