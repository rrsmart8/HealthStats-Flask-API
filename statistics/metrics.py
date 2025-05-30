import matplotlib.pyplot as plt

# Results from your benchmark:
sync_time = 20.07
async_time = 4.02
sync_ram = 500
async_ram = 200

# Plot execution time
plt.figure(figsize=(6, 4))
plt.bar(['Synchronous', 'Asynchronous'], [sync_time, async_time], color=['red', 'green'])
plt.title('Execution Time: Synchronous vs Asynchronous')
plt.ylabel('Total time (seconds)')
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig('execution_time_comparison.png')
plt.show()

# Plot RAM usage
plt.figure(figsize=(6, 4))
plt.bar(['Synchronous', 'Asynchronous'], [sync_ram, async_ram], color=['red', 'green'])
plt.title('Estimated RAM Usage: Synchronous vs Asynchronous')
plt.ylabel('RAM usage (MB)')
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig('ram_usage_comparison.png')
plt.show()