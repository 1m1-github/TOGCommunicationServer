module TOGCommunicationServer

using ZMQ
using LoopOS: InputPeripheral, OutputPeripheral, listen, @whiletrue
using TOGZMQ #, TOGZMQClient

# const GROUP = Dict{String,String}()
const GROUPS = Set{String}()
const ROUTERSOCKET = Ref{Socket}()
const PUBSOCKET = Ref{Socket}()

# function addclient(;group, name)
#     to ∈ GROUPS && error("$name already exists as a client")
#     push!(GROUPS, to)
#     # TOGZMQClient.start(group=group, router=ROUTERSOCKET[].last_endpoint, pub=PUBSOCKET[].last_endpoint, name=name)
# end

# todo creategroup, addtogroup, rmfromgroup, rmgroup

function awaken(;router, pub)
    ROUTERSOCKET[] = Socket(ROUTER)
    PUBSOCKET[] = Socket(PUB)
    bind(ROUTERSOCKET[], router)
    bind(PUBSOCKET[], pub)
    # listen(RECEIVE)
    @async @whiletrue begin
        to, from, symbol, description, information = TOGZMQ.receive(ROUTERSOCKET[])
        # haskey(AGENT, from) || continue
        TOGZMQ.send(PUBSOCKET[], to, from, information)
        # if to == "group"
        if to ∈ GROUPS
            # group = AGENTGROUP[from]
            TOGZMQ.send(PUBSOCKET[], [to, from, symbol, description, information])
            # send_multipart(PUBSOCKET[], [group, from, message])
        # elseif AGENTGROUP[from] == get(AGENTGROUP, to, "")
        else
            TOGZMQ.send(ROUTERSOCKET[], [to, from, symbol, description, information])
            # send_multipart(ROUTERSOCKET[], [to, from, message])
        end
    end
end

# import Base: take!, put!
# send(socket, message, to) = ZMQ.send_multipart(socket, [to, Sys.username(), message])
# put!(::DirectMessage, message::String, to::String) = send(ROUTERSOCKET[], message, to)
# put!(::GroupMessage, message::String, to::String="∀") = send(PUBSOCKET[], message, to)

end
