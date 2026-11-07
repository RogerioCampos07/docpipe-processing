"""Provision the isolated Processing AMQP user and optional input exchange."""

import asyncio
import base64
import json
import os
import re
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import Request, urlopen

import aio_pika
from aio_pika import ExchangeType


class ProvisioningError(RuntimeError):
    """RabbitMQ provisioning failed without a safe public diagnostic."""


MINIMUM_WORKER_PASSWORD_LENGTH = 8


def _required(name: str) -> str:
    value = os.getenv(name, '').strip()
    if not value:
        raise ProvisioningError(f'{name} must be configured.')
    return value


def _admin_base_and_auth() -> tuple[str, tuple[str, str]]:
    value = _required('PROCESSING_AMQP_ADMIN_HTTP_URL')
    parsed = urlsplit(value)
    if parsed.scheme not in {'http', 'https'} or not parsed.hostname:
        raise ProvisioningError(
            'PROCESSING_AMQP_ADMIN_HTTP_URL must be an HTTP management URL.'
        )
    if parsed.username is None or parsed.password is None:
        raise ProvisioningError(
            'The RabbitMQ management URL must include admin credentials.'
        )
    host = parsed.hostname
    if ':' in host:
        host = f'[{host}]'
    port = f':{parsed.port}' if parsed.port is not None else ''
    return (
        f'{parsed.scheme}://{host}{port}',
        (unquote(parsed.username), unquote(parsed.password)),
    )


def _management_put(
    base_url: str,
    credentials: tuple[str, str],
    path: str,
    body: dict[str, object],
) -> None:
    user, password = credentials
    token = base64.b64encode(f'{user}:{password}'.encode()).decode('ascii')
    request = Request(
        f'{base_url}/api/{path}',
        data=json.dumps(body).encode(),
        headers={
            'Authorization': f'Basic {token}',
            'Content-Type': 'application/json',
        },
        method='PUT',
    )
    try:
        with urlopen(request, timeout=10) as response:
            if response.status not in {201, 204}:
                raise ProvisioningError(
                    'RabbitMQ management rejected provisioning.'
                )
    except HTTPError, URLError, TimeoutError:
        raise ProvisioningError(
            'RabbitMQ management provisioning did not complete.'
        ) from None


def _resource_pattern(*names: str) -> str:
    return '^(' + '|'.join(re.escape(name) for name in names) + ')$'


async def provision() -> None:
    base_url, admin_credentials = _admin_base_and_auth()
    admin_amqp_url = _required('PROCESSING_AMQP_ADMIN_URL')
    worker_name = _required('PROCESSING_AMQP_WORKER_USER')
    worker_password = _required('PROCESSING_AMQP_WORKER_PASSWORD')
    virtual_host = os.getenv('PROCESSING_AMQP_VHOST', 'processing').strip()
    if len(worker_password) < MINIMUM_WORKER_PASSWORD_LENGTH:
        raise ProvisioningError(
            'The Processing worker password must contain at least '
            f'{MINIMUM_WORKER_PASSWORD_LENGTH} characters.'
        )

    exchange = os.getenv('PROCESSING_AMQP_EXCHANGE', 'docpipe.events').strip()
    exchange_type = os.getenv(
        'PROCESSING_AMQP_EXCHANGE_TYPE', 'direct'
    ).strip()
    queue = os.getenv(
        'PROCESSING_AMQP_QUEUE', 'processing.document-received.v1'
    ).strip()
    dead_letter_exchange = os.getenv(
        'PROCESSING_AMQP_DLX', 'processing.dlx.v1'
    ).strip()
    dead_letter_queue = os.getenv(
        'PROCESSING_AMQP_DLQ', 'processing.dead-letter.v1'
    ).strip()

    user_path = quote(worker_name, safe='')
    vhost_path = quote(virtual_host, safe='')
    _management_put(
        base_url,
        admin_credentials,
        f'users/{user_path}',
        {'password': worker_password, 'tags': ''},
    )
    _management_put(
        base_url,
        admin_credentials,
        f'permissions/{vhost_path}/{user_path}',
        {
            'configure': _resource_pattern(
                queue,
                dead_letter_exchange,
                dead_letter_queue,
            ),
            'write': _resource_pattern(exchange, dead_letter_exchange),
            'read': _resource_pattern(exchange, queue),
        },
    )

    if (
        os.getenv('PROCESSING_AMQP_PROVISION_INPUT_EXCHANGE', '').lower()
        == 'true'
    ):
        connection = await aio_pika.connect(admin_amqp_url, timeout=10)
        try:
            channel = await connection.channel(publisher_confirms=False)
            await channel.declare_exchange(
                exchange,
                type=ExchangeType(exchange_type),
                durable=True,
            )
        finally:
            await connection.close()


def main() -> None:
    asyncio.run(provision())


if __name__ == '__main__':
    main()
