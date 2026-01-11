import { Realtime } from '../src/server/realtime';
import RedisMock from 'ioredis-mock';
import { z } from 'zod';

const NUM_MESSAGES = 10000;
const CHANNEL_NAME = 'benchmark-channel';

function printMemory(label: string) {
  const used = process.memoryUsage();
  console.log(`\n📊 Memory [${label}]:`);
  console.log(`   RSS: ${(used.rss / 1024 / 1024).toFixed(2)} MB`);
  console.log(`   Heap Total: ${(used.heapTotal / 1024 / 1024).toFixed(2)} MB`);
  console.log(`   Heap Used: ${(used.heapUsed / 1024 / 1024).toFixed(2)} MB`);
}

async function runBenchmark() {
  console.log('🚀 Starting Stress Test...');
  console.log(`📝 Target: ${NUM_MESSAGES} messages`);
  
  if (global.gc) {
      global.gc();
  }
  printMemory('Start');

  // Setup
  const redis = new RedisMock();
  const schema = {
    chat: {
      message: z.object({
        text: z.string(),
        timestamp: z.number(),
      })
    }
  };

  const realtime = new Realtime({
    redis: redis as any,
    schema,
    verbose: false // Disable logging to measure pure throughput
  });

  const channel = realtime.channel(CHANNEL_NAME);
  
  // 1. Measure Emit Speed
  console.log('\n--- Emit Performance ---');
  const startEmit = performance.now();
  
  for (let i = 0; i < NUM_MESSAGES; i++) {
    await channel.emit('chat.message', {
      text: `Message ${i}`,
      timestamp: Date.now()
    });
  }

  const endEmit = performance.now();
  const durationEmit = (endEmit - startEmit) / 1000; // seconds
  const rateEmit = NUM_MESSAGES / durationEmit;
  
  console.log(`✅ Emitted ${NUM_MESSAGES} messages in ${durationEmit.toFixed(3)}s`);
  console.log(`⚡ Rate: ${rateEmit.toFixed(0)} messages/sec`);
  
  printMemory('After Emit');

  // 2. Measure History Retrieval Speed
  console.log('\n--- History Performance ---');
  const startHistory = performance.now();
  
  const history = await channel.history({ limit: NUM_MESSAGES });
  
  const endHistory = performance.now();
  const durationHistory = (endHistory - startHistory) / 1000;
  
  console.log(`✅ Retrieved ${history.length} messages in ${durationHistory.toFixed(3)}s`);
  
  // Verify data integrity
  if (history.length !== NUM_MESSAGES) {
      console.error(`❌ Data Mismatch! Expected ${NUM_MESSAGES}, got ${history.length}`);
  } else {
      console.log(`✨ Data integrity check passed.`);
  }
  
  if (global.gc) {
      global.gc();
      printMemory('After GC');
  } else {
      printMemory('End (No GC)');
      console.log('ℹ️  Run with --expose-gc to see post-GC memory stats');
  }

  console.log('\n🏁 Benchmark Complete');
}

runBenchmark().catch(console.error);
