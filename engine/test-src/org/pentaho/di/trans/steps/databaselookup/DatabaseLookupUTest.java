package org.pentaho.di.trans.steps.databaselookup;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mock.StepMockHelper;
import org.pentaho.metastore.api.IMetaStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.pentaho.di.core.row.ValueMetaInterface.STORAGE_TYPE_BINARY_STRING;
import static org.pentaho.di.core.row.ValueMetaInterface.TYPE_STRING;

/**
 * @author Andrey Khayrutdinov
 */
public class DatabaseLookupUTest {

  private static final String BINARY_FIELD = "aBinaryFieldInDb";
  private static final String ID_FIELD = "id";

  @Test
  public void testMySqlBinaryIsInterpretedAsString() throws Exception {
    StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> mockHelper = createMockHelper();
    DatabaseLookupMeta meta = createDatabaseMeta();
    DatabaseLookupData data = createDatabaseData();
    Database db = createVirtualDb( meta.getDatabaseMeta() );

    DatabaseLookup lookup = spyLookup( mockHelper, db, meta.getDatabaseMeta() );

    lookup.init( meta, data );
    lookup.processRow( meta, data );

    ValueMetaInterface binaryFieldMeta = data.outputRowMeta.getValueMeta( 0 );
    assertThat( binaryFieldMeta.getType(), is( equalTo( TYPE_STRING ) ) );
    assertThat( binaryFieldMeta.getStorageType(), is( equalTo( STORAGE_TYPE_BINARY_STRING ) ) );
  }


  private StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> createMockHelper() {
    StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> mockHelper =
      new StepMockHelper<DatabaseLookupMeta, DatabaseLookupData>( "test DatabaseLookup", DatabaseLookupMeta.class,
        DatabaseLookupData.class );
    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mockHelper.logChannelInterface );
    when( mockHelper.trans.isRunning() ).thenReturn( true );

    RowMeta inputRowMeta = new RowMeta();
    RowSet rowSet = mock( RowSet.class );
    when( rowSet.getRowWait( anyLong(), any( TimeUnit.class ) ) ).thenReturn( new Object[] { } ).thenReturn( null );
    when( rowSet.getRowMeta() ).thenReturn( inputRowMeta );

    when( mockHelper.trans.findRowSet( anyString(), anyInt(), anyString(), anyInt() ) ).thenReturn( rowSet );

    when( mockHelper.transMeta.findNextSteps( Matchers.any( StepMeta.class ) ) )
      .thenReturn( singletonList( mock( StepMeta.class ) ) );
    when( mockHelper.transMeta.findPreviousSteps( any( StepMeta.class ), anyBoolean() ) )
      .thenReturn( singletonList( mock( StepMeta.class ) ) );

    return mockHelper;
  }


  private DatabaseLookupMeta createDatabaseMeta() throws KettleException {
    DatabaseInterface dbInterface = mock( DatabaseInterface.class );
    when( dbInterface.isMySQLVariant() ).thenReturn( true );

    DatabaseMeta dbMeta = mock( DatabaseMeta.class );
    when( dbMeta.getDatabaseInterface() ).thenReturn( dbInterface );

    DatabaseLookupMeta meta = new DatabaseLookupMeta();
    meta.setDatabaseMeta( dbMeta );
    meta.setTablename( "VirtualTable" );

    meta.setTableKeyField( new String[] { ID_FIELD } );
    meta.setKeyCondition( new String[] { "=" } );

    meta.setReturnValueNewName( new String[] { "returned value" } );
    meta.setReturnValueField( new String[] { BINARY_FIELD } );
    meta.setReturnValueDefaultType( new int[] { ValueMetaInterface.TYPE_BINARY } );

    meta.setStreamKeyField1( new String[ 0 ] );
    meta.setStreamKeyField2( new String[ 0 ] );

    meta.setReturnValueDefault( new String[] { "" } );

    meta = spy( meta );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        RowMetaInterface row = (RowMetaInterface) invocation.getArguments()[ 0 ];
        ValueMetaInterface v = new ValueMetaBinary( BINARY_FIELD );
        row.addValueMeta( v );
        return null;
      }
    } ).when( meta ).getFields(
      any( RowMetaInterface.class ),
      anyString(),
      any( RowMetaInterface[].class ),
      any( StepMeta.class ),
      any( VariableSpace.class ),
      any( Repository.class ),
      any( IMetaStore.class )
    );
    return meta;
  }


  private DatabaseLookupData createDatabaseData() {
    return new DatabaseLookupData();
  }


  private Database createVirtualDb( DatabaseMeta meta ) throws Exception {
    ResultSet rs = mock( ResultSet.class );
    when( rs.getMetaData() ).thenReturn( mock( ResultSetMetaData.class ) );

    PreparedStatement ps = mock( PreparedStatement.class );
    when( ps.executeQuery() ).thenReturn( rs );

    Connection connection = mock( Connection.class );
    when( connection.prepareStatement( anyString() ) ).thenReturn( ps );

    Database db = new Database( mock( LoggingObjectInterface.class ), meta );
    db.setConnection( connection );

    db = spy( db );
    doNothing().when( db ).normalConnect( anyString() );

    ValueMetaInterface binary = new ValueMetaString( BINARY_FIELD );
    binary.setStorageType( ValueMetaInterface.STORAGE_TYPE_BINARY_STRING );

    ValueMetaInterface id = new ValueMetaInteger( ID_FIELD );

    RowMetaInterface metaByQuerying = new RowMeta();
    metaByQuerying.addValueMeta( binary );
    metaByQuerying.addValueMeta( id );

    doReturn( metaByQuerying ).when( db ).getTableFields( anyString() );

    return db;
  }


  private DatabaseLookup spyLookup( StepMockHelper<DatabaseLookupMeta, DatabaseLookupData> mocks, Database db,
                                    DatabaseMeta dbMeta ) {
    DatabaseLookup lookup =
      new DatabaseLookup( mocks.stepMeta, mocks.stepDataInterface, 1, mocks.transMeta, mocks.trans );
    lookup = Mockito.spy( lookup );

    doReturn( db ).when( lookup ).getDatabase( eq( dbMeta ) );
    for ( RowSet rowSet : lookup.getOutputRowSets() ) {
      if ( mockingDetails( rowSet ).isMock() ) {
        when( rowSet.putRow( any( RowMetaInterface.class ), any( Object[].class ) ) ).thenReturn( true );
      }
    }

    return lookup;
  }
}
