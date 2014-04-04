/*
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2014 by Pentaho : http://www.pentaho.com
 *
 * **************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pentaho.di.core.database;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.DelegatingConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.util.DatabaseUtil;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;


public class PoolableDataSourceTest {

  private static long MAX_WAIT_TIME = 1000;
  private static int MAX_ACTIVE = 3;
  private static String VALIDATION_QUERY = "select 1 from INFORMATION_SCHEMA.SYSTEM_USERS";

  private LogChannelInterface logChannelInterface;
  DatabaseMeta dbMeta;


  @Before
  public void setUp() throws Exception {
    logChannelInterface = mock( LogChannelInterface.class, RETURNS_MOCKS );
    KettleEnvironment.init();
    dbMeta = new DatabaseMeta( "CP3", "Hypersonic", "JDBC", null, "mem:test", null, null, null );
    Properties dsProps = new Properties();
    dsProps.setProperty( ConnectionPoolUtil.DEFAULT_AUTO_COMMIT, "true" );
    dsProps.setProperty( ConnectionPoolUtil.DEFAULT_READ_ONLY, "true" );
    dsProps.setProperty( ConnectionPoolUtil.DEFAULT_TRANSACTION_ISOLATION, "1" );
    dsProps.setProperty( ConnectionPoolUtil.DEFAULT_CATALOG, "" );
    dsProps.setProperty( ConnectionPoolUtil.MAX_IDLE, "30" );
    dsProps.setProperty( ConnectionPoolUtil.MIN_IDLE, "3" );
    dsProps.setProperty( ConnectionPoolUtil.MAX_WAIT, String.valueOf( MAX_WAIT_TIME ) ); // tested
    dsProps.setProperty( ConnectionPoolUtil.VALIDATION_QUERY, VALIDATION_QUERY );
    dsProps.setProperty( ConnectionPoolUtil.TEST_ON_BORROW, "true" );
    dsProps.setProperty( ConnectionPoolUtil.TEST_ON_RETURN, "true" );
    dsProps.setProperty( ConnectionPoolUtil.TEST_WHILE_IDLE, "true" );
    dsProps.setProperty( ConnectionPoolUtil.TIME_BETWEEN_EVICTION_RUNS_MILLIS, "300000" );
    dsProps.setProperty( ConnectionPoolUtil.POOL_PREPARED_STATEMENTS, "true" ); // tested
    dsProps.setProperty( ConnectionPoolUtil.MAX_OPEN_PREPARED_STATEMENTS, "2" ); // tested
    dsProps.setProperty( ConnectionPoolUtil.ACCESS_TO_UNDERLYING_CONNECTION_ALLOWED, "true" ); // tested
    dsProps.setProperty( ConnectionPoolUtil.REMOVE_ABANDONED, "false" );
    dsProps.setProperty( ConnectionPoolUtil.REMOVE_ABANDONED_TIMEOUT, "1000" );
    dsProps.setProperty( ConnectionPoolUtil.LOG_ABANDONED, "false" );
    dbMeta.setConnectionPoolingProperties( dsProps );
    dbMeta.setUsername( "SA" );
    dbMeta.setPassword( "" );
    dbMeta.setInitialPoolSize( 1 );
    dbMeta.setMaximumPoolSize( MAX_ACTIVE ); // tested
  }

  @After
  public void tearDown() throws Exception {
  }


  @Test
  public void testPropertiesAreSet() throws Exception {
    Connection conn = null;
    try {
      conn = getConnection();
      Field field = ConnectionPoolUtil.class.getDeclaredField( "dataSources" );
      assertNotNull( "Can't find field 'dataSources' in class ConnectionPoolUtil", field );
      field.setAccessible( true );
      Map<String, BasicDataSource> dataSources = (Map<String, BasicDataSource>) field.get( ConnectionPoolUtil.class );
      BasicDataSource ds = dataSources.get( dbMeta.getName() );

      assertEquals( true, ds.getDefaultAutoCommit() );
      assertEquals( true, ds.getDefaultReadOnly() );
      assertEquals( 1, ds.getDefaultTransactionIsolation() );
      assertEquals( null, ds.getDefaultCatalog() );
      assertEquals( 30, ds.getMaxIdle() );
      assertEquals( 3, ds.getMinIdle() );
      assertEquals( MAX_WAIT_TIME, ds.getMaxWait() );
      assertEquals( VALIDATION_QUERY, ds.getValidationQuery() );
      assertEquals( true, ds.getTestOnBorrow() );
      assertEquals( true, ds.getTestOnReturn() );
      assertEquals( true, ds.getTestWhileIdle() );
      assertEquals( 300000, ds.getTimeBetweenEvictionRunsMillis() );
      assertEquals( true, ds.isPoolPreparedStatements() );
      assertEquals( 2, ds.getMaxOpenPreparedStatements() );
      assertEquals( true, ds.isAccessToUnderlyingConnectionAllowed() );
      assertEquals( false, ds.getRemoveAbandoned() );
      assertEquals( 1000, ds.getRemoveAbandonedTimeout() );
      assertEquals( false, ds.getLogAbandoned() );
    } finally {
      DatabaseUtil.closeSilently( conn );
    }
  }

  @Test
  public void testPreparedStatementsProperty() throws Exception {
    Connection conn = null;
    PreparedStatement[] ps = new PreparedStatement[ 3 ];
    try {

      conn = getConnection();
      ps[ 0 ] = conn.prepareStatement( VALIDATION_QUERY );
      ps[ 1 ] = conn.prepareStatement( VALIDATION_QUERY );
      boolean failed = false;
      try {
        ps[ 2 ] = conn.prepareStatement( VALIDATION_QUERY );
      } catch ( Exception e ) {
        failed = true;
      }
      assertTrue( "Properties 'poolPreparedStatements' or 'maxOpenPreparedStatements' don't work", failed );
    } finally {
      DatabaseUtil.closeSilently( ps );
      DatabaseUtil.closeSilently( conn );
    }
  }

  // maxPoolSize is set to "3", maxWait = MAX_WAIT_TIME
  // so after getting the next connection the thread should block for 3 seconds and then
  // throw "Cannot get connection exception"
  @Test( timeout = 6000 )
  public void testMaxActiveProperty() throws Exception {
    Connection[] c = new Connection[ 4 ];
    try {
      c[ 0 ] = getConnection();
      c[ 1 ] = getConnection();
      c[ 2 ] = getConnection();
      long startTime = System.currentTimeMillis();
      try {
        // this must wait a bit and throw an exception
        c[ 3 ] = getConnection();
      } catch ( SQLException e ) {
        long waitedTime = System.currentTimeMillis() - startTime;
        assertFalse( "Waited < maxWait", waitedTime < MAX_WAIT_TIME );
      }
    } finally {
      DatabaseUtil.closeSilently( c );
    }
  }

  @Test
  public void testAccessToUnderlyingConnectionAllowedProperty() throws Exception {
    Connection conn = null;
    try {
      conn = getConnection();
      Connection dconn = ( (DelegatingConnection) conn ).getInnermostDelegate();
      assertNotNull( "Property 'accessToUnderlyingConnectionAllowed' doesn't work", dconn );
    } catch ( Exception e ) {
      fail();
    }
  }

  private Connection getConnection() throws Exception {
    return ConnectionPoolUtil.getConnection( logChannelInterface, dbMeta, "" );
  }


}
