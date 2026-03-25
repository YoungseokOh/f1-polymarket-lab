export type ResourceState<T> = {
  data: T;
  degraded: boolean;
  error: string | null;
};

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return "unknown error";
}

export async function loadResource<T>(
  loader: () => Promise<T>,
  fallback: T,
  label: string,
): Promise<ResourceState<T>> {
  try {
    return {
      data: await loader(),
      degraded: false,
      error: null,
    };
  } catch (error) {
    return {
      data: fallback,
      degraded: true,
      error: `${label} unavailable: ${errorMessage(error)}`,
    };
  }
}

export function collectResourceErrors(
  states: Array<ResourceState<unknown>>,
): string[] {
  return states
    .map((state) => state.error)
    .filter((error): error is string => Boolean(error));
}
