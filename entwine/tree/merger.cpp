/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/builder.hpp>
#include <entwine/tree/merger.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/manifest.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Merger::Merger(const Config& config)
    : m_config(config)
{
    m_outerScope.getArbiter(config.arbiter());
    m_config.setSubsetId(0);
    m_builder = Builder::tryCreateExisting(m_config, m_outerScope);

    if (!m_builder)
    {
        throw std::runtime_error("Path not mergeable");
    }
    else if (!m_builder->metadata().subset())
    {
        throw std::runtime_error(
                "This path is already whole - no merge needed");
    }

    m_outerScope.setPointPool(m_builder->sharedPointPool());
    m_outerScope.setHierarchyPool(m_builder->sharedHierarchyPool());

    if (const Subset* subset = m_builder->metadata().subset())
    {
        m_config.setSubsetOf(subset->of());
    }
    else
    {
        throw std::runtime_error("Could not get number of subsets");
    }

    if (m_config.verbose())
    {
        std::cout << "Awakened 1 / " << m_config.of() << std::endl;
    }
}

Merger::~Merger() { }

void Merger::go()
{
    m_builder->unbump();

    Pool pool(m_config.threads());
    std::size_t fetches(static_cast<float>(pool.size()) * 1.2);

    while (m_config.id() < m_config.of())
    {
        const std::size_t n(std::min(fetches, m_config.of() - m_config.id()));
        std::vector<std::unique_ptr<Builder>> builders(n);

        for (auto& b : builders)
        {
            m_config.setSubsetId(m_config.id() + 1);

            if (m_config.verbose())
            {
                std::cout << "Merging " << m_config.id() << " / " <<
                    m_config.of() << std::endl;
            }

            const Config copy(m_config);
            pool.add([this, &b, copy]()
            {
                b = Builder::tryCreateExisting(copy, m_outerScope);
                if (!b) std::cout << "Failed: " << copy.id() << std::endl;
            });
        }

        pool.cycle();

        for (auto& b : builders)
        {
            if (!b) throw std::runtime_error("Couldn't create subset");
            m_builder->merge(*b);
        }
    }

    m_builder->makeWhole();

    if (m_config.verbose()) std::cout << "Merged.  Saving..." << std::endl;
    m_builder->save();
    m_builder.reset();
    if (m_config.verbose()) std::cout << "\tFinal save complete." << std::endl;
}

} // namespace entwine

