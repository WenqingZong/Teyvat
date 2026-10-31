# ClickHouse（RedmiBook）手动安装记录

开发期间库结构还会反复推倒重来，所以先不脚本化，只记录 RedmiBook 上做过什么、留下了什么、怎么清干净。开发期间的数据都可以丢。

## 现状

- 版本：26.8 LTS，官方 apt 源，`apt-mark hold` 锁版本。
- systemd：`clickhouse-server.service`（包自带），已 enable。
- 只监听 `127.0.0.1:8123`（HTTP）和 `127.0.0.1:9000`（原生协议）。
- 库：`phanes`。账号：`default` 已移除；`phanes`（`phanes.*` 全部权限）；`grafana`（只读，`readonly=2`）。

## 安装步骤

```sh
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' \
  | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=$(dpkg --print-architecture)] https://packages.clickhouse.com/deb lts main" \
  | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
sudo apt-get install -y clickhouse-server clickhouse-client   # default 密码提示直接回车
sudo apt-mark hold clickhouse-server clickhouse-client clickhouse-common-static
sudo systemctl stop clickhouse-server   # 写好配置前不要启动，否则会建出系统日志表
```

然后写下面的配置文件（属主 `clickhouse:clickhouse`，权限 400），再 `sudo systemctl start clickhouse-server`，最后 `clickhouse-client -q "CREATE DATABASE IF NOT EXISTS phanes"`。

## 留下的文件

### `/etc/clickhouse-server/config.d/`

`network.xml`

```xml
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <!-- Single instance: no MySQL / PostgreSQL wire protocol, no replication. -->
    <mysql_port remove="1"/>
    <postgresql_port remove="1"/>
    <interserver_http_port remove="1"/>
</clickhouse>
```

`memory.xml`

```xml
<clickhouse>
    <!-- ~3 GB of 7.6 GB; the laptop also runs Phanes, Grafana, backtests and a desktop. -->
    <max_server_memory_usage_to_ram_ratio>0.4</max_server_memory_usage_to_ram_ratio>
    <mark_cache_size>268435456</mark_cache_size>
    <uncompressed_cache_size>134217728</uncompressed_cache_size>
</clickhouse>
```

`logging.xml`

```xml
<clickhouse>
    <logger>
        <level>information</level>
        <size>100M</size>
        <count>3</count>
    </logger>
    <timezone>UTC</timezone>
</clickhouse>
```

`system_logs.xml`：关掉所有系统日志表，名单从本机 `config.xml` 生成（版本不同名单不同）：

```sh
{ echo '<clickhouse>'; sudo grep -oP '^    <\K[a-z_]+_log(?=>)' /etc/clickhouse-server/config.xml | sort -u | sed 's|.*|    <& remove="1"/>|'; echo '</clickhouse>'; } | sudo tee /etc/clickhouse-server/config.d/system_logs.xml
```

### `/etc/clickhouse-server/users.d/phanes.xml`

```xml
<clickhouse>
    <profiles>
        <grafana>
            <readonly>2</readonly>
        </grafana>
    </profiles>
    <users>
        <default remove="1"/>
        <phanes>
            <password_sha256_hex><!-- sha256 of the phanes password --></password_sha256_hex>
            <networks><ip>127.0.0.1</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <grants><query>GRANT ALL ON phanes.*</query></grants>
        </phanes>
        <grafana>
            <password_sha256_hex><!-- sha256 of the grafana password --></password_sha256_hex>
            <networks><ip>127.0.0.1</ip></networks>
            <profile>grafana</profile>
            <quota>default</quota>
            <grants><query>GRANT SELECT ON phanes.*</query></grants>
        </grafana>
    </users>
</clickhouse>
```

密码用 `openssl rand -hex 24` 生成，哈希用 `tr -d '\n' < 密码文件 | sha256sum | cut -d' ' -f1`。

### `~/.clickhouse-client/`（600）

- `config.xml`：`<config><user>phanes</user><password>…</password></config>`，`clickhouse-client` 默认以 `phanes` 登录。Phanes 入库代码写好后，这个密码要放进 `phanes.toml`。
- `grafana.password`：配 Grafana 数据源后删除。

### 其他

- 数据：`/var/lib/clickhouse/`
- 日志：`/var/log/clickhouse-server/`
- apt 源：`/etc/apt/sources.list.d/clickhouse.list`、`/usr/share/keyrings/clickhouse-keyring.gpg`
- 系统用户：`clickhouse`（包创建）

## 重新初始化（保留安装和配置，清空所有数据）

```sh
sudo systemctl stop clickhouse-server
sudo find /var/lib/clickhouse -mindepth 1 -delete
sudo find /var/log/clickhouse-server -type f -delete
sudo systemctl start clickhouse-server
clickhouse-client -q "CREATE DATABASE IF NOT EXISTS phanes"
```

改了 `config.d/` 或 `users.d/` 只需 `sudo systemctl restart clickhouse-server`。

## 完全卸载

```sh
sudo systemctl disable --now clickhouse-server
sudo apt-mark unhold clickhouse-server clickhouse-client clickhouse-common-static
sudo apt-get purge -y clickhouse-server clickhouse-client clickhouse-common-static
sudo rm -rf /etc/clickhouse-server /etc/clickhouse-client /var/lib/clickhouse /var/log/clickhouse-server
sudo rm -f /etc/apt/sources.list.d/clickhouse.list /usr/share/keyrings/clickhouse-keyring.gpg
sudo apt-get update
getent passwd clickhouse && sudo userdel clickhouse
getent group clickhouse && sudo groupdel clickhouse
rm -rf ~/.clickhouse-client
```

## 踩过的坑

- 不要调小 `background_pool_size`：MergeTree 默认的 `number_of_free_entries_in_pool_to_execute_mutation`（20）、`..._optimize_entire_partition`（25）不能超过 `background_pool_size × 2`，否则启动时报错退出。
- `config.d/`、`users.d/` 只有 clickhouse 能读，通配符要在 root 下展开：`sudo sh -c 'chmod 400 /etc/clickhouse-server/config.d/*.xml'`。
- 用 `sudo -u clickhouse clickhouse extract-from-config --config-file /etc/clickhouse-server/config.xml --key <key>` 可以在不启动的情况下检查合并后的配置（账号用 `users.xml`）。

## 未做

- Transparent hugepages 仍为 `always`，ClickHouse 建议 `madvise`。
