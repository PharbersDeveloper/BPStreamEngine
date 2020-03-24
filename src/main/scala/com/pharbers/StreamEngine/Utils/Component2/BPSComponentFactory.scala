package com.pharbers.StreamEngine.Utils.Component2

trait BPSComponentFactory extends BPComponent {
    entry: BPSEntry =>
//    override val config: BPConfig = null
    val context = new BPSComponentContext

    def getOrCreateInstance(cf: BPComponentConfig): BPComponent = {
        container.get(cf.id) match {
            case Some(ins) => ins
            case None => context.buildComponent(cf)
        }
    }
}
