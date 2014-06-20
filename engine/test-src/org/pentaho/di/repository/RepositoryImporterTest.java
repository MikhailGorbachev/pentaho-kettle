package org.pentaho.di.repository;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@RunWith( MockitoJUnitRunner.class )
public class RepositoryImporterTest {

  private static final String ROOT_PATH = "/test_root";

  private static final String USER_NAME_PATH = "/userName";

  @Mock
  private RepositoryImportFeedbackInterface feedback;

  @Mock
  private RepositoryDirectoryInterface baseDirectory;

  private Node jobnode;

  @Before
  public void beforeTest() {
    jobnode = mock( Node.class );
    NodeList nodeList = mock( NodeList.class );
    when( jobnode.getChildNodes() ).thenReturn( nodeList );
  }

  @Test
  public void testImportJob_patchJobEntries_without_variables() throws KettleException {
    JobEntryTrans jobEntryTrans = createJobEntryTrans( "/userName" );
    RepositoryImporter importer = createRepositoryImporter( jobEntryTrans, new Variables() );
    when( baseDirectory.getPath() ).thenReturn( ROOT_PATH );
    importer.setBaseDirectory( baseDirectory );

    importer.importJob( jobnode, feedback );
    verify( jobEntryTrans ).setDirectory( ROOT_PATH + USER_NAME_PATH );
  }

  @Test
  public void testImportJob_patchJobEntries_with_variable() throws KettleException {
    JobEntryTrans jobEntryTrans = createJobEntryTrans( "${USER_VARIABLE}" );
    VariableSpace variableSpace = new Variables();
    variableSpace.setVariable( "USER_VARIABLE", "userName" );
    RepositoryImporter importer = createRepositoryImporter( jobEntryTrans, variableSpace );
    when( baseDirectory.getPath() ).thenReturn( ROOT_PATH );
    importer.setBaseDirectory( baseDirectory );

    importer.importJob( jobnode, feedback );
    String expectedPath = ROOT_PATH + "/" + "${USER_VARIABLE}";
    verify( jobEntryTrans ).setDirectory( expectedPath );
  }

  @Test
  public void testImportJob_patchJobEntries_with_variable_containing_the_leading_slash() throws KettleException {
    JobEntryTrans jobEntryTrans = createJobEntryTrans( "${USER_VARIABLE}" );
    VariableSpace variableSpace = new Variables();
    variableSpace.setVariable( "USER_VARIABLE", "/userName" );
    RepositoryImporter importer = createRepositoryImporter( jobEntryTrans, variableSpace );
    when( baseDirectory.getPath() ).thenReturn( ROOT_PATH );
    importer.setBaseDirectory( baseDirectory );

    importer.importJob( jobnode, feedback );
    String expectedPath = ROOT_PATH + "${USER_VARIABLE}";
    verify( jobEntryTrans ).setDirectory( expectedPath );
  }

  @Test
  public void testImportJob_patchJobEntries_with_variable_equal_to_a_slash() throws KettleException {
    JobEntryTrans jobEntryTrans = createJobEntryTrans( "${USER_VARIABLE}" );
    VariableSpace variableSpace = new Variables();
    variableSpace.setVariable( "USER_VARIABLE", "/" );
    RepositoryImporter importer = createRepositoryImporter( jobEntryTrans, variableSpace );
    when( baseDirectory.getPath() ).thenReturn( ROOT_PATH );
    importer.setBaseDirectory( baseDirectory );

    importer.importJob( jobnode, feedback );
    String expectedPath = ROOT_PATH;
    verify( jobEntryTrans ).setDirectory( expectedPath );
  }

  private static JobEntryTrans createJobEntryTrans( String directory ) {
    JobEntryTrans jet = mock( JobEntryTrans.class );
    when( jet.getSpecificationMethod() ).thenReturn( ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME );
    when( jet.getDirectory() ).thenReturn( directory );
    return jet;
  }

  private static RepositoryImporter createRepositoryImporter( final JobEntryTrans jobEntryTrans,
      final VariableSpace variableSpace ) {
    Repository repository = mock( Repository.class );
    LogChannelInterface log = mock( LogChannelInterface.class );
    RepositoryImporter importer = new RepositoryImporter( repository, log ) {
      @Override
      JobMeta createJobMetaForNode( Node jobnode ) throws KettleXMLException {
        JobMeta meta = mock( JobMeta.class );
        meta.initializeVariablesFrom( variableSpace );
        doAnswer( new Answer<String>() {

          @Override
          public String answer( InvocationOnMock invocation ) throws Throwable {
            return variableSpace.environmentSubstitute( (String) invocation.getArguments()[0] );
          }
        } ).when( meta ).environmentSubstitute( anyString() );
        JobEntryCopy jec = mock( JobEntryCopy.class );
        when( jec.isTransformation() ).thenReturn( true );
        when( jec.getEntry() ).thenReturn( jobEntryTrans );
        when( meta.getJobCopies() ).thenReturn( Collections.singletonList( jec ) );
        return meta;
      }

      @Override
      protected void replaceSharedObjects( JobMeta transMeta ) throws KettleException {
      }
    };
    return importer;
  }

}
