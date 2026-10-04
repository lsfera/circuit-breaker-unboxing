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

export const assertSupportedRabbitMqVersion = (version: string): void => {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?$/.exec(version);
  if (!match) throw new UnsupportedRabbitMqVersionError(version);

  const found = match.slice(1).map(Number);
  if (found.some((part) => !Number.isSafeInteger(part))) throw new UnsupportedRabbitMqVersionError(version);

  for (let i = 0; i < MINIMUM_VERSION.length; i++) {
    if (found[i]! > MINIMUM_VERSION[i]!) return;
    if (found[i]! < MINIMUM_VERSION[i]!) throw new UnsupportedRabbitMqVersionError(version);
  }
};
