import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node', // Default to node
    // Make sure we include all test files
    include: ['**/*.{test,spec}.{ts,tsx}'],
  },
});
