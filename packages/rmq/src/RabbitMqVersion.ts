const MINIMUM_VERSION = [4, 3, 0] as const;
export const MINIMUM_RABBITMQ_VERSION = MINIMUM_VERSION.join(".");

export class UnsupportedRabbitMqVersionError extends Error {
  constructor(version: string) {
    const detected = version
      ? `detected ${JSON.stringify(version)}`
      : "could not determine the broker version";
    super(
      `Unsupported RabbitMQ version (${detected}). This application requires RabbitMQ ${MINIMUM_RABBITMQ_VERSION} or newer; upgrade the broker and restart.`
    );
    this.name = "UnsupportedRabbitMqVersionError";
  }
}

/** `major.minor.patch` of a broker's version string, or `undefined` for anything this cannot verify. */
const parse = (version: string): ReadonlyArray<number> | undefined => {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?$/.exec(version);
  const found = match?.slice(1).map(Number);
  return found?.every(Number.isSafeInteger) ? found : undefined;
};

const atLeast = (found: ReadonlyArray<number>, minimum: ReadonlyArray<number>): boolean => {
  for (let i = 0; i < minimum.length; i++) {
    if (found[i]! > minimum[i]!) return true;
    if (found[i]! < minimum[i]!) return false;
  }
  return true;
};

export const assertSupportedRabbitMqVersion = (version: string): void => {
  const found = parse(version);
  if (found === undefined || !atLeast(found, MINIMUM_VERSION)) throw new UnsupportedRabbitMqVersionError(version);
};

/**
 * Whether `version` is `minimum` (`"4.4.0"`) or newer. A version string this cannot verify, or an empty one (the
 * broker's was never read), is not: a feature gated on this stays off unless the broker is known to have it.
 */
export const rabbitMqVersionAtLeast = (version: string, minimum: string): boolean => {
  const found = parse(version);
  const wanted = parse(minimum);
  return found !== undefined && wanted !== undefined && atLeast(found, wanted);
};
