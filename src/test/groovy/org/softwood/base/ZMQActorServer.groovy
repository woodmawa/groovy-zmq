package org.softwood.base

import org.zeromq.*

class GameMaster extends ZMQActor {
    int secretNum

    public GameMaster(ZMQ.Context ctx, String addr) {
        super(ctx, ZMQ.REP)
        bind(addr)
        secretNum = new Random().nextInt(10)
    }

    void act() {
        loop {
            react { num ->
                num = num.toInteger()
                if (num > secretNum)
                    reply 'too large'
                else if (num < secretNum)
                    reply 'too small'
                else {
                    reply 'you win'
                    terminate()
                }
            }
        }
    }
}

def ctx = ZMQ.context(1)
new GameMaster(ctx, 'tcp://*:54321').start()