package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceImplBase;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 *
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 *
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService extends DhtServiceImplBase {

    private static final String TAG = NodeService.class.getCanonicalName();

    private static Logger logger = Logger.getLogger(TAG);

    /**
     * Each service request is processed by a distinct service object.
     * <p>
     * Shared state is in the state object; we use the singleton pattern to make sure it is shared.
     */
    private Dht getDht() {
        return Dht.getDht();
    }


    // TODO: add the missing operations

    private void error(String mesg, Exception e) {
        logger.log(Level.SEVERE, mesg, e);
    }

    @Override
    public void getNodeInfo(Empty empty, StreamObserver<NodeInfo> responseObserver) {
        Log.weblog(TAG, "getNodeInfo()");
        responseObserver.onNext(getDht().getNodeInfo());
        responseObserver.onCompleted();
    }

    @Override
    public void getPred(Empty empty, StreamObserver<OptNodeInfo> responseObserver) {
        Log.weblog(TAG, "getPred()");
        // An empty OptNodeInfo means "no predecessor". Never send null.
        responseObserver.onNext(getDht().getPred());
        responseObserver.onCompleted();
    }


    @Override
    public void getSucc(Empty empty, StreamObserver<NodeInfo> responseObserver) {
        Log.weblog(TAG, "getSucc()");
        responseObserver.onNext(getDht().getSucc());
        responseObserver.onCompleted();
    }


    @Override
    public void closestPrecedingFinger(Id request, StreamObserver<NodeInfo> responseObserver) {
        Log.weblog(TAG, "closestPrecedingFinger(" + request.getId() + ")");
        responseObserver.onNext(getDht().closestPrecedingFinger(request.getId()));
        responseObserver.onCompleted();
    }

    @Override
    public void findSuccessor(Id request, StreamObserver<NodeInfo> responseObserver) {
        Log.weblog(TAG, "findSuccessor(" + request.getId() + ")");

        try {
            responseObserver.onNext(getDht().findSuccessor(request.getId()));
        } catch (Failed e) {
            throw new RuntimeException(e);
        }
        responseObserver.onCompleted();

    }

    @Override
    public void notify(NodeBindings request, StreamObserver<OptNodeBindings> responseObserver) {
        Log.weblog(TAG, "notify(" + request.getInfo().getId() + ")");
        // Dht.notify returns the empty default instance when it refuses the
        // candidate. Pass it through unchanged; the client treats it as "refused".
        responseObserver.onNext(getDht().notify(request));
        responseObserver.onCompleted();
    }

    @Override
    public void getBindings(Key request, StreamObserver<Bindings> responseObserver) {
        Log.weblog(TAG, "getBindings(" + request.getKey() + ")");
        try {
            String[] values = getDht().get(request.getKey());
            responseObserver.onNext(Bindings.newBuilder()
                    .setKey(request.getKey())
                    .addAllValue(Arrays.asList(values))
                    .build());
            responseObserver.onCompleted();
        } catch (Invalid e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addBinding(Binding request, StreamObserver<Empty> responseObserver) {
        Log.weblog(TAG, "addBinding(" + request.getKey() + ", " + request.getValue() + ")");
        try {
            getDht().add(request.getKey(), request.getValue());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Invalid e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteBinding(Binding request, StreamObserver<Empty> responseObserver) {
        Log.weblog(TAG, "deleteBinding(" + request.getKey() + ", " + request.getValue() + ")");
        try {
            getDht().delete(request.getKey(), request.getValue());
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Invalid e) {
            throw new RuntimeException(e);
        }
    }
}