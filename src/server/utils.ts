export function compareStreamIds(a: string, b: string): number {
  const [aTime = 0, aSeq = 0] = a.split("-").map(Number);
  const [bTime = 0, bSeq = 0] = b.split("-").map(Number);

  if (aTime !== bTime) return aTime - bTime;
  return aSeq - bSeq;
}

export function parseStreamResponse(
  response: unknown
): Record<string, unknown>[] {
  if (!Array.isArray(response)) return [];

  return response.map((item) => {
    const id = item[0] as string;
    const fields = item[1] as string[];
    const data: Record<string, unknown> = {};

    if (Array.isArray(fields)) {
      for (let i = 0; i < fields.length; i += 2) {
        const key = fields[i];
        if (typeof key === "string") {
          data[key] = fields[i + 1];
        }
      }
    }

    // Try to parse 'data' field if it is a JSON string
    if (typeof data.data === "string") {
      try {
        data.data = JSON.parse(data.data);
      } catch {
        // ignore
      }
    }

    return { ...data, id };
  });
}
