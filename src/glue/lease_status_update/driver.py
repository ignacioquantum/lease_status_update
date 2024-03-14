from src.glue.lease_status_update.lease_status_update import LeaseStatusUpdater



def main():

    driver = LeaseStatusUpdater('quantum_mysql_prod')
    secret = driver.secret

    print('Estableciendo tunel ssh.')
    # Crear una instancia de SSHTunnelForwarder para gestionar el túnel SSH
    with SSHTunnelForwarder((secret['SSH_HOST'], int(secret['SSH_PORT'])),
                            ssh_username=secret['SSH_USER'],
                            ssh_password=secret['SSH_PASSWORD'],
                            remote_bind_address=(secret['DB_HOST'], int(secret['DB_PORT'])),
                            local_bind_address=('localhost', int(secret['DB_PORT']))) as tunnel:

        print('Tunnel SSH establecido correctamente.')
        driver.main()


if __name__ == "__main__":
    main()