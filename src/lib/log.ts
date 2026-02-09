export const log = {
  info: (message: string, meta?: unknown): void => {
    if (meta === undefined) {
      console.info(`[info] ${message}`);
      return;
    }
    console.info(`[info] ${message}`, meta);
  },
  warn: (message: string, meta?: unknown): void => {
    if (meta === undefined) {
      console.warn(`[warn] ${message}`);
      return;
    }
    console.warn(`[warn] ${message}`, meta);
  },
  error: (message: string, meta?: unknown): void => {
    if (meta === undefined) {
      console.error(`[error] ${message}`);
      return;
    }
    console.error(`[error] ${message}`, meta);
  }
};
