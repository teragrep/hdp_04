/*
 * Execution Requisites HDP-04
 * Copyright (C) 2025 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.hdp_04;

import com.teragrep.cnf_01.ConfigurationException;
import com.teragrep.cnf_01.PathConfiguration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import javax.naming.NamingException;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final int secretLength = 20;

    public static void main(final String[] args) throws ConfigurationException {
        final String configurationPath = System.getProperty("configurationPath", "etc/configuration.properties");
        final Map<String, String> config = new PathConfiguration(configurationPath).asMap();
        LOGGER.info("Using configuration: <[{}]>", configurationPath);

        final Hdfs hdfs = new Hdfs(
                config.get("hdp_03_conf_dir"),
                config.get("hdfs_principal"),
                config.get("hdfs_keytab")
        );

        final List<String> users = new ArrayList<>();

        try {
            final Ldap ldap = new Ldap(
                    config.get("ldap_url"),
                    config.get("ldap_principal"),
                    config.get("ldap_password"),
                    config.get("ldap_filter"),
                    config.get("ldap_dn")
            );
            users.addAll(ldap.fetchUsers());
            LOGGER.info("Found <{}> users", users.size());
        }
        catch (NamingException namingException) {
            LOGGER.error("Unable to get LDAP users: <[{}]>", namingException.getMessage(), namingException);
            System.exit(1);
        }

        try (FileSystem hdfsConnection = hdfs.get()) {
            LOGGER.info("Checking if /user exists");
            final Path home = new Path("/user");
            if (!hdfsConnection.exists(home)) {
                LOGGER.info("Creating /user");
                hdfsConnection.mkdirs(home);
                hdfsConnection
                        .setPermission(
                                home, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE, true)
                        );
                hdfsConnection.setOwner(home, "hdfs", "hadoop");
            }

            for (String user : users) {
                final Path userHome = new Path("/user/", user);
                LOGGER.info("Checking if <{}> exists", userHome);
                if (!hdfsConnection.exists(userHome)) {
                    LOGGER.info("Creating <{}>", userHome);
                    hdfsConnection.mkdirs(userHome);
                    hdfsConnection
                            .setPermission(userHome, new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE, true));
                    hdfsConnection.setOwner(userHome, user, "hadoop");
                }
                final Path userSecret = new Path(userHome, "s3credential");
                LOGGER.info("Checking if <{}> exists", userSecret);
                if (!hdfsConnection.exists(userSecret)) {
                    LOGGER.info("Creating <{}>", userSecret);
                    try (FSDataOutputStream fsDataOutputStream = hdfsConnection.create(userSecret)) {
                        fsDataOutputStream
                                .write(RandomStringUtils.random(secretLength, 0, 0, true, true, null, new SecureRandom()).getBytes());
                    }
                    hdfsConnection.setOwner(userSecret, user, "hadoop");
                    hdfsConnection
                            .setPermission(
                                    userSecret, new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE, true)
                            );
                }
            }
        }
        catch (IOException ioException) {
            LOGGER.error("Unable to connect to hdfs: <[{}]>", ioException.getMessage(), ioException);
            System.exit(1);
        }
    }
}
