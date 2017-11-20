========
autosync
========

ZFS snapshot replication tool

Installation
------------

 - ``cd /opt``
 - ``git clone https://github.com/makhomed/autosync.git autosync``

Upgrade
-------

 - ``cd /opt/autosync``
 - ``git pull``

Configuration
-------------

  - ``vim /opt/autosync/source-server.conf``
  - write to config something like this:

.. code-block:: none

    source source-server.example.com

    exclude tank
    exclude tank/backup**
    exclude tank/vm

    destination tank/mirror

Configuration file allow comments, from symbol ``#`` to end of line.

Configuration file has only five directives:
``source``, ``exclude``, ``include``, ``destination`` and ``delay``.

Syntax of ``source`` directive: ``source <source-server>``.
``<source-server>`` is hostname of source server or it ip address.

Syntax of ``include`` and ``exclude`` directives are the same:
``exclude <pattern>`` or ``include <pattern>``.

By default all datasets are included. But you can exclude some datasets
by name or by pattern. Pattern is rsync-like, ``?`` means any one symbol,
``*`` means any symbols except ``/`` symbol, ``**`` means any symbols.

First match win, and if it was directive ``exclude`` - dataset will be excluded,
if it was directive ``include`` - dataset will be included.

``exclude`` and ``include`` define datasets for replication from source server.

``destination`` directive define destination dataset name on current server.

``delay`` defines delay in seconds between two sequential run of sync. Default value is 600 seconds.


Secure Shell
------------

For work you need to generate private ssh key on destination server
with comamnd ``ssh-keygen -t rsa`` and copy public key from ``/root/.ssh/id_rsa.pub``
to ``/root/.ssh/authorized_keys`` on source servers. Also you need to check connection
with command ``ssh source-server.example.com`` and answer ``yes`` on ssh question:

.. code-block:: none

    # ssh source-server.example.com
    The authenticity of host 'source-server.example.com' can't be established.
    ECDSA key fingerprint is SHA256:/cYI0bJzEX+CF3DhGEUQ+ZeGFmMzEJYAt3C15450zKs.
    ECDSA key fingerprint is MD5:44:20:bd:f5:aa:a7:52:ac:c5:19:e5:e0:28:2b:90:49.
    Are you sure you want to continue connecting (yes/no)? yes


Systemd Service
---------------

  - ``vim /etc/systemd/system/autosync@.service``
  - write to unit file something like this:

.. code-block:: none

    [Unit]
    Description=autosync %I
    After=network-online.target

    [Service]
    ExecStart=/opt/autosync/autosync -c /opt/autosync/%i.conf
    Restart=always

    [Install]
    WantedBy=multi-user.target

After this you need to start service:

  - ``systemctl daemon-reload``
  - ``systemctl enable autosync@source-server``
  - ``systemctl start autosync@source-server``
  - ``systemctl status autosync@source-server``

If all ok you will see what service is enabled and running.

``autosync`` will scan datasets on source server and automatically mirror
all enabled by include/exclude rules datasets to destination dataset on local server.

Safe time to stop service ``autosync`` - when it in idle state, i.e. when
command ``systemctl status autosync@source-server`` show no child processes.

